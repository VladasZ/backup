use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::digest::Digest;
use super::files::{list, read, sweep_stale_partials, write_atomic};
use super::pack::{PackEntry, read_directory};
use super::seal::{seal, unseal};

pub const PACKS: &str = "packs";
pub const INDEX: &str = "index";
const PACK_SUFFIX: &str = ".pack";
const INDEX_SUFFIX: &str = ".index";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexedPack {
    pub id: Digest,
    pub entries: Vec<PackEntry>,
}

/// Which pack holds which chunk. Index files are only a cache of the pack
/// directories: a pack that no index file lists is read and added on load, so
/// losing every index file loses nothing.
#[derive(Default)]
pub struct Index {
    packs: HashMap<Digest, Vec<PackEntry>>,
    chunks: HashMap<Digest, Vec<Digest>>,
    files: Vec<PathBuf>,
}

impl Index {
    pub fn load(root: &Path) -> Result<Self> {
        let mut index = Self::default();
        let directory = root.join(INDEX);
        sweep_stale_partials(&directory);
        for path in list(&directory, INDEX_SUFFIX)? {
            match read_index_file(&path) {
                Ok(packs) => {
                    for pack in packs {
                        index.add(pack);
                    }
                }
                Err(error) => {
                    warn!(path = %path.display(), error = %format!("{error:#}"), "skipping a damaged index file; its packs are read directly");
                }
            }
            index.files.push(path);
        }
        let mut recovered = Vec::new();
        for (id, path) in pack_files(root)? {
            if index.packs.contains_key(&id) {
                continue;
            }
            match read_pack(&path, id) {
                Ok(payload) => {
                    let pack = IndexedPack {
                        id,
                        entries: read_directory(&payload)?,
                    };
                    recovered.push(pack.clone());
                    index.add(pack);
                }
                Err(error) => {
                    warn!(path = %path.display(), error = %format!("{error:#}"), "could not read a pack that no index lists");
                }
            }
        }
        if !recovered.is_empty() {
            info!(root = %root.display(), packs = recovered.len(), "indexed packs that no index file listed");
            let path = write_index(root, &recovered)?;
            index.files.push(path);
        }
        Ok(index)
    }

    pub fn add(&mut self, pack: IndexedPack) {
        if self.packs.contains_key(&pack.id) {
            return;
        }
        for entry in &pack.entries {
            self.chunks.entry(entry.id).or_default().push(pack.id);
        }
        self.packs.insert(pack.id, pack.entries);
    }

    pub fn remove_pack(&mut self, id: &Digest) {
        let Some(entries) = self.packs.remove(id) else {
            return;
        };
        for entry in entries {
            if let Some(packs) = self.chunks.get_mut(&entry.id) {
                packs.retain(|pack| pack != id);
                if packs.is_empty() {
                    self.chunks.remove(&entry.id);
                }
            }
        }
    }

    pub fn contains(&self, chunk: &Digest) -> bool {
        self.chunks.contains_key(chunk)
    }

    pub fn packs_of(&self, chunk: &Digest) -> Vec<Digest> {
        self.chunks.get(chunk).cloned().unwrap_or_default()
    }

    pub fn entry(&self, pack: &Digest, chunk: &Digest) -> Option<&PackEntry> {
        self.packs
            .get(pack)?
            .iter()
            .find(|entry| entry.id == *chunk)
    }

    pub fn entries(&self, pack: &Digest) -> Option<&[PackEntry]> {
        self.packs.get(pack).map(Vec::as_slice)
    }

    pub fn pack_ids(&self) -> Vec<Digest> {
        let mut ids: Vec<Digest> = self.packs.keys().copied().collect();
        ids.sort();
        ids
    }

    pub fn chunk_ids(&self) -> impl Iterator<Item = &Digest> {
        self.chunks.keys()
    }

    pub fn packs(&self) -> Vec<IndexedPack> {
        self.pack_ids()
            .into_iter()
            .filter_map(|id| {
                self.packs.get(&id).map(|entries| IndexedPack {
                    id,
                    entries: entries.clone(),
                })
            })
            .collect()
    }

    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    pub fn add_file(&mut self, path: PathBuf) {
        if !self.files.contains(&path) {
            self.files.push(path);
        }
    }

    pub fn replace_files(&mut self, files: Vec<PathBuf>) {
        self.files = files;
    }
}

pub fn pack_path(root: &Path, id: &Digest) -> PathBuf {
    let hex = id.to_hex();
    root.join(PACKS)
        .join(&hex[..2])
        .join(format!("{hex}{PACK_SUFFIX}"))
}

pub fn write_index(root: &Path, packs: &[IndexedPack]) -> Result<PathBuf> {
    let payload = serde_json::to_vec(packs).context("encode index")?;
    let path = root
        .join(INDEX)
        .join(format!("{}{INDEX_SUFFIX}", Digest::of(&payload).to_hex()));
    write_atomic(&path, &seal(&payload)?)?;
    Ok(path)
}

/// The pack payload, checked against the name it is stored under. A pack
/// that needed parity repair is rewritten healed.
pub fn read_pack(path: &Path, id: Digest) -> Result<Vec<u8>> {
    let unsealed = unseal(&read(path)?).with_context(|| format!("unseal {}", path.display()))?;
    if Digest::of(&unsealed.payload) != id {
        bail!("pack {} does not match its name", path.display());
    }
    if unsealed.repaired > 0 {
        warn!(path = %path.display(), pieces = unsealed.repaired, "repaired a damaged pack from its parity");
        write_atomic(path, &seal(&unsealed.payload)?)?;
    }
    Ok(unsealed.payload)
}

fn read_index_file(path: &Path) -> Result<Vec<IndexedPack>> {
    let unsealed = unseal(&read(path)?).with_context(|| format!("unseal {}", path.display()))?;
    let packs = serde_json::from_slice(&unsealed.payload)
        .with_context(|| format!("decode {}", path.display()))?;
    if unsealed.repaired > 0 {
        warn!(path = %path.display(), pieces = unsealed.repaired, "repaired a damaged index file from its parity");
        write_atomic(path, &seal(&unsealed.payload)?)?;
    }
    Ok(packs)
}

pub fn pack_files(root: &Path) -> Result<Vec<(Digest, PathBuf)>> {
    let packs = root.join(PACKS);
    let shards = match fs::read_dir(&packs) {
        Ok(shards) => shards,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("read directory {}", packs.display()));
        }
    };
    let mut found = Vec::new();
    for shard in shards {
        let shard = shard.with_context(|| format!("read directory {}", packs.display()))?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        sweep_stale_partials(&shard.path());
        for path in list(&shard.path(), PACK_SUFFIX)? {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(PACK_SUFFIX))
                .map(str::to_owned);
            match name.as_deref().map(Digest::parse) {
                Some(Ok(id)) => found.push((id, path)),
                _ => warn!(path = %path.display(), "ignoring a pack file with an unexpected name"),
            }
        }
    }
    found.sort();
    Ok(found)
}
