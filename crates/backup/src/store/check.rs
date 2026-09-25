use std::collections::{BTreeSet, HashSet};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::digest::Digest;
use super::gc::retire_packs;
use super::index::{pack_path, read_pack};
use super::pack::extract;
use super::recipe::Recipe;
use super::{Store, StoreWriter};

/// The result of reading everything in a store. Parity damage is healed on
/// the way, so what is listed here is damage the store cannot fix alone.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StoreCheck {
    pub packs: usize,
    pub intact: Vec<String>,
    pub damaged_packs: Vec<Digest>,
    pub bad_chunks: Vec<Digest>,
    pub bad_recipes: Vec<String>,
    pub incomplete: Vec<String>,
}

impl StoreCheck {
    pub fn is_clean(&self) -> bool {
        self.damaged_packs.is_empty()
            && self.bad_chunks.is_empty()
            && self.bad_recipes.is_empty()
            && self.incomplete.is_empty()
    }
}

pub fn check_store(store: &mut Store) -> Result<StoreCheck> {
    let root = store.root().to_path_buf();
    let packs = store.index()?.packs();
    let mut good = HashSet::new();
    let mut damaged = BTreeSet::new();
    for pack in &packs {
        let payload = match read_pack(&pack_path(&root, &pack.id), pack.id) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(pack = %pack.id, error = %format!("{error:#}"), "pack is damaged beyond its parity");
                damaged.insert(pack.id);
                continue;
            }
        };
        for entry in &pack.entries {
            match extract(&payload, entry) {
                Ok(_) => {
                    good.insert(entry.id);
                }
                Err(error) => {
                    warn!(pack = %pack.id, error = %format!("{error:#}"), "chunk is damaged");
                    damaged.insert(pack.id);
                }
            }
        }
    }
    store.cached = None;

    let mut check = StoreCheck {
        packs: packs.len(),
        damaged_packs: damaged.into_iter().collect(),
        ..StoreCheck::default()
    };
    let mut bad = BTreeSet::new();
    for name in store.recipe_names()? {
        match store.read_recipe(&name) {
            Ok(recipe) => {
                let missing: Vec<Digest> = recipe
                    .chunks
                    .iter()
                    .map(|chunk| chunk.id)
                    .filter(|id| !good.contains(id))
                    .collect();
                if missing.is_empty() {
                    check.intact.push(name);
                } else {
                    bad.extend(missing);
                    check.incomplete.push(name);
                }
            }
            Err(error) => {
                warn!(recipe = name, error = %format!("{error:#}"), "recipe is damaged beyond its parity");
                check.bad_recipes.push(name);
            }
        }
    }
    check.bad_chunks = bad.into_iter().collect();
    Ok(check)
}

/// Finishes a repair once good copies of the bad chunks were written. Lost
/// recipes are written back, then each damaged pack has its intact chunks
/// copied out and is deleted, but only when every chunk it held now has an
/// intact copy somewhere else in the store.
pub fn finish_repair(mut store: Store, damaged: &[Digest], recipes: &[Recipe]) -> Result<Store> {
    for recipe in recipes {
        store.ensure_complete(recipe)?;
        store.write_recipe(recipe)?;
        info!(recipe = recipe.name, destination = %store.root().display(), "restored a damaged recipe");
    }
    let root = store.root().to_path_buf();
    let damaged_set: HashSet<Digest> = damaged.iter().copied().collect();
    let mut writer = store.into_writer();
    let mut retired = Vec::new();
    for pack in damaged {
        let Some(entries) = writer.store.index()?.entries(pack).map(<[_]>::to_vec) else {
            continue;
        };
        let payload = read_pack(&pack_path(&root, pack), *pack).ok();
        let mut safe = true;
        for entry in &entries {
            let elsewhere = writer
                .store
                .index()?
                .packs_of(&entry.id)
                .iter()
                .any(|other| !damaged_set.contains(other));
            if elsewhere {
                continue;
            }
            match payload.as_deref().map(|payload| extract(payload, entry)) {
                Some(Ok(bytes)) => writer.put(&entry.id, &bytes)?,
                _ => safe = false,
            }
        }
        if safe {
            retired.push(*pack);
        } else {
            warn!(%pack, "keeping a damaged pack, some of its chunks have no other copy");
        }
    }
    let store = writer.finish()?;
    retire_packs(store, &retired)
}

/// Writes one good copy of a chunk that a check found bad.
pub fn put_repaired(writer: &mut StoreWriter, id: &Digest, bytes: &[u8]) -> Result<()> {
    if Digest::of(bytes) != *id {
        bail!("replacement for chunk {id} does not match its hash");
    }
    writer.put(id, bytes)
}
