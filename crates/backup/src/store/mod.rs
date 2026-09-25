//! A destination folder is a content-addressed store. Every backup is a recipe
//! of chunks, and each chunk is kept once in a pack however many backups use
//! it. Packs, index files and recipes are all sealed with parity.

pub mod check;
pub mod digest;
pub mod files;
pub mod gc;
pub mod import;
pub mod index;
pub mod pack;
pub mod recipe;
pub mod seal;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::fs;
use std::mem::take;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tracing::warn;

use crate::archive::parse_archive_name;
use digest::Digest;
use files::{list, read, remove_if_present, sweep_stale_partials, write_atomic};
use index::{Index, IndexedPack, pack_path, read_pack, write_index};
use pack::{PackBuilder, extract};
use recipe::{Recipe, RecipeInfo};
use seal::{seal, unseal};

pub const RECIPES: &str = "recipes";
const RECIPE_SUFFIX: &str = ".recipe";

pub struct Store {
    root: PathBuf,
    index: Option<Index>,
    cached: Option<(Digest, Vec<u8>)>,
}

impl Store {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("create destination {}", root.display()))?;
        if !fs::metadata(root)?.is_dir() {
            bail!("destination {} is not a directory", root.display());
        }
        sweep_stale_partials(root);
        sweep_stale_partials(&root.join(RECIPES));
        Ok(Self {
            root: root.to_path_buf(),
            index: None,
            cached: None,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn index(&mut self) -> Result<&mut Index> {
        if self.index.is_none() {
            self.index = Some(Index::load(&self.root)?);
        }
        self.index.as_mut().context("index was not loaded")
    }

    pub fn contains(&mut self, chunk: &Digest) -> Result<bool> {
        Ok(self.index()?.contains(chunk))
    }

    pub fn chunk_ids(&mut self) -> Result<HashSet<Digest>> {
        Ok(self.index()?.chunk_ids().copied().collect())
    }

    /// The chunk bytes from the first pack that holds an intact copy.
    pub fn read_chunk(&mut self, chunk: &Digest) -> Result<Vec<u8>> {
        let packs = self.index()?.packs_of(chunk);
        if packs.is_empty() {
            bail!("chunk {chunk} is not in {}", self.root.display());
        }
        let mut failures = Vec::new();
        for pack in packs {
            match self.chunk_from(pack, chunk) {
                Ok(bytes) => return Ok(bytes),
                Err(error) => failures.push(format!("{error:#}")),
            }
        }
        bail!(
            "chunk {chunk} is damaged in {}: {}",
            self.root.display(),
            failures.join("; ")
        )
    }

    fn chunk_from(&mut self, pack: Digest, chunk: &Digest) -> Result<Vec<u8>> {
        if self.cached.as_ref().is_none_or(|(id, _)| *id != pack) {
            self.cached = None;
            let payload = read_pack(&pack_path(&self.root, &pack), pack)?;
            self.cached = Some((pack, payload));
        }
        let entry = self
            .index()?
            .entry(&pack, chunk)
            .cloned()
            .with_context(|| format!("pack {pack} does not list chunk {chunk}"))?;
        let (_, payload) = self.cached.as_ref().context("pack cache is empty")?;
        extract(payload, &entry)
    }

    pub fn into_writer(self) -> StoreWriter {
        StoreWriter {
            store: self,
            builder: PackBuilder::default(),
            added: HashSet::new(),
            written: Vec::new(),
        }
    }

    pub fn recipe_path(&self, name: &str) -> PathBuf {
        self.root
            .join(RECIPES)
            .join(format!("{name}{RECIPE_SUFFIX}"))
    }

    pub fn has_recipe(&self, name: &str) -> bool {
        self.recipe_path(name).exists()
    }

    /// Names of every recipe in the folder, whatever job wrote it.
    pub fn recipe_names(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for path in list(&self.root.join(RECIPES), RECIPE_SUFFIX)? {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(RECIPE_SUFFIX))
                .filter(|name| parse_archive_name(name).is_some());
            match name {
                Some(name) => names.push(name.to_owned()),
                None => warn!(path = %path.display(), "ignoring a recipe with an unexpected name"),
            }
        }
        Ok(names)
    }

    /// Recipes of one job, or of all jobs, newest first.
    pub fn list_recipes(&self, job: Option<&str>) -> Result<Vec<RecipeInfo>> {
        let mut infos = Vec::new();
        for name in self.recipe_names()? {
            let belongs = parse_archive_name(&name)
                .is_some_and(|parsed| job.is_none_or(|job| parsed.job == job));
            if !belongs {
                continue;
            }
            infos.push(self.read_recipe(&name)?.info());
        }
        infos.sort_by(|left, right| {
            right
                .created
                .cmp(&left.created)
                .then_with(|| right.name.cmp(&left.name))
        });
        Ok(infos)
    }

    pub fn read_recipe(&self, name: &str) -> Result<Recipe> {
        if parse_archive_name(name).is_none() {
            bail!("{name:?} is not an archive name");
        }
        let path = self.recipe_path(name);
        let unsealed =
            unseal(&read(&path)?).with_context(|| format!("unseal {}", path.display()))?;
        let recipe: Recipe = serde_json::from_slice(&unsealed.payload)
            .with_context(|| format!("decode {}", path.display()))?;
        if recipe.name != name {
            bail!("{} holds the recipe of {}", path.display(), recipe.name);
        }
        if unsealed.repaired > 0 {
            warn!(path = %path.display(), pieces = unsealed.repaired, "repaired a damaged recipe from its parity");
            write_atomic(&path, &seal(&unsealed.payload)?)?;
        }
        Ok(recipe)
    }

    pub fn write_recipe(&self, recipe: &Recipe) -> Result<()> {
        if parse_archive_name(&recipe.name).is_none_or(|parsed| parsed.job != recipe.job) {
            bail!(
                "recipe name {:?} does not belong to job {:?}",
                recipe.name,
                recipe.job
            );
        }
        let payload = serde_json::to_vec(recipe).context("encode recipe")?;
        write_atomic(&self.recipe_path(&recipe.name), &seal(&payload)?)
    }

    pub fn remove_recipe(&self, name: &str) -> Result<()> {
        if parse_archive_name(name).is_none() {
            bail!("{name:?} is not an archive name");
        }
        remove_if_present(&self.recipe_path(name))
    }

    /// Every recipe chunk is present, so writing the recipe cannot publish a
    /// backup that does not restore.
    pub fn ensure_complete(&mut self, recipe: &Recipe) -> Result<()> {
        let index = self.index()?;
        let missing = recipe
            .chunks
            .iter()
            .filter(|chunk| !index.contains(&chunk.id))
            .count();
        if missing > 0 {
            bail!(
                "{missing} chunk(s) of {} are missing from {}",
                recipe.name,
                self.root.display()
            );
        }
        Ok(())
    }
}

/// Adds chunks to a store in new packs. Nothing is visible to readers until
/// `finish` writes the index file for the new packs.
pub struct StoreWriter {
    store: Store,
    builder: PackBuilder,
    added: HashSet<Digest>,
    written: Vec<IndexedPack>,
}

impl StoreWriter {
    pub fn root(&self) -> &Path {
        self.store.root()
    }

    pub fn contains(&mut self, chunk: &Digest) -> Result<bool> {
        Ok(self.added.contains(chunk) || self.store.contains(chunk)?)
    }

    pub fn put(&mut self, chunk: &Digest, plain: &[u8]) -> Result<()> {
        if !self.added.insert(*chunk) {
            return Ok(());
        }
        self.builder.add(*chunk, plain)?;
        if self.builder.is_full() {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let pack = take(&mut self.builder).seal()?;
        write_atomic(&pack_path(self.store.root(), &pack.id), &pack.bytes)?;
        self.written.push(IndexedPack {
            id: pack.id,
            entries: pack.entries,
        });
        Ok(())
    }

    pub fn finish(mut self) -> Result<Store> {
        self.flush()?;
        if !self.written.is_empty() {
            let file = write_index(self.store.root(), &self.written)?;
            let index = self.store.index()?;
            index.add_file(file);
            for pack in self.written {
                index.add(pack);
            }
        }
        Ok(self.store)
    }
}
