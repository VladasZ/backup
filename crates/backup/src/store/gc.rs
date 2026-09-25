use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use tracing::{info, warn};

use super::Store;
use super::digest::Digest;
use super::files::{modified_before, remove_if_present};
use super::index::{pack_path, read_pack, write_index};
use super::pack::extract;
use crate::config::BackupJob;
use crate::retention::retention_removals;

// A pack younger than this may belong to a run that has not written its
// recipe yet, so cleanup leaves it alone even when no recipe uses it.
const YOUNG_PACK_AGE: Duration = Duration::from_secs(60 * 60);

// A pack whose live bytes fall below this share is rewritten, so deleted
// backups give their space back without rewriting packs that are mostly live.
const REPACK_LIVE_PERCENT: u64 = 70;

// More index files than this are merged into one during cleanup.
const MAX_INDEX_FILES: usize = 16;

pub fn apply_retention(store: Store, job: &BackupJob) -> Result<Store> {
    // Cleanup also runs for a job that keeps everything, since an interrupted
    // run can still leave packs that no backup uses.
    let Some(retention) = &job.retention else {
        return collect_garbage(store);
    };
    let recipes = store.list_recipes(Some(&job.name))?;
    let created: Vec<_> = recipes.iter().map(|recipe| recipe.created).collect();
    for index in retention_removals(&created, retention, Utc::now())? {
        let recipe = &recipes[index];
        store.remove_recipe(&recipe.name)?;
        info!(
            job = job.name,
            archive = recipe.name,
            destination = %store.root().display(),
            "removed archive due to retention"
        );
    }
    collect_garbage(store)
}

/// Deletes packs no recipe uses and rewrites packs that are mostly unused.
/// Every recipe in the folder counts, whatever job wrote it, since jobs can
/// share a destination and its chunks.
///
/// A chunk stored in several packs, for example by two writers that ran at
/// once, counts only in one of them, its keeper, so the extra copies are freed
/// like unused data. A copy is dropped only after the keeper's copy was read
/// back intact.
pub fn collect_garbage(mut store: Store) -> Result<Store> {
    let mut live = HashSet::new();
    for name in store.recipe_names()? {
        let recipe = store.read_recipe(&name).with_context(|| {
            format!("read recipe {name}; cleanup is skipped so its chunks are not deleted")
        })?;
        live.extend(recipe.chunks.iter().map(|chunk| chunk.id));
    }
    let cutoff = SystemTime::now() - YOUNG_PACK_AGE;
    let root = store.root().to_path_buf();
    let mut dead = Vec::new();
    let mut sparse = Vec::new();
    {
        let index = store.index()?;
        let keeper = |id: &Digest| index.packs_of(id).into_iter().min();
        for pack in index.packs() {
            if !modified_before(&pack_path(&root, &pack.id), cutoff)? {
                continue;
            }
            let total: u64 = pack
                .entries
                .iter()
                .map(|entry| u64::from(entry.stored))
                .sum();
            let used: u64 = pack
                .entries
                .iter()
                .filter(|entry| live.contains(&entry.id) && keeper(&entry.id) == Some(pack.id))
                .map(|entry| u64::from(entry.stored))
                .sum();
            if used == 0 {
                dead.push(pack.id);
            } else if used * 100 < total * REPACK_LIVE_PERCENT {
                sparse.push(pack.id);
            }
        }
    }
    if dead.is_empty() && sparse.is_empty() && store.index()?.files().len() <= MAX_INDEX_FILES {
        return Ok(store);
    }

    let mut retired = Vec::new();
    let mut writer = store.into_writer();
    for pack in dead.into_iter().chain(sparse) {
        let entries = writer
            .store
            .index()?
            .entries(&pack)
            .map(<[_]>::to_vec)
            .unwrap_or_default();
        let mut payload = None;
        let mut safe = true;
        for entry in entries.iter().filter(|entry| live.contains(&entry.id)) {
            let keeper = writer.store.index()?.packs_of(&entry.id).into_iter().min();
            if keeper != Some(pack) && writer.store.read_chunk_from(keeper, &entry.id).is_ok() {
                continue;
            }
            if payload.is_none() {
                payload = Some(read_pack(&pack_path(&root, &pack), pack));
            }
            let copied = match payload.as_ref() {
                Some(Ok(payload)) => {
                    extract(payload, entry).and_then(|bytes| writer.put(&entry.id, &bytes))
                }
                Some(Err(error)) => Err(anyhow!("{error:#}")),
                None => Err(anyhow!("pack {pack} was not read")),
            };
            if let Err(error) = copied {
                warn!(%pack, error = %format!("{error:#}"), "cannot move a chunk out of a pack; verify will repair it");
                safe = false;
            }
        }
        if safe {
            retired.push(pack);
        }
    }
    let store = writer.finish()?;
    let freed = retired.len();
    let store = retire_packs(store, &retired)?;
    info!(
        destination = %root.display(),
        packs = freed,
        "cleanup removed or rewrote packs"
    );
    Ok(store)
}

/// Drops packs from the index and deletes their files. A merged index is
/// written first and the old index files go next, so a crash at any point
/// leaves every live chunk reachable. A pack file that outlives its index
/// entry is indexed again on the next load and cleaned up later.
pub(super) fn retire_packs(mut store: Store, packs: &[Digest]) -> Result<Store> {
    let root = store.root().to_path_buf();
    let index = store.index()?;
    for pack in packs {
        index.remove_pack(pack);
    }
    let merged = write_index(&root, &index.packs())?;
    for file in index.files().to_vec() {
        if file != merged {
            remove_if_present(&file)?;
        }
    }
    index.replace_files(vec![merged]);
    for pack in packs {
        remove_if_present(&pack_path(&root, pack))?;
    }
    store.cached = None;
    Ok(store)
}
