//! Gives the backups of a job a new job name inside one destination. Packs and
//! index files do not know the job, only the recipes do, by their file name and
//! inside. Every step finds what an earlier, broken run already did, so a
//! rename that stopped halfway finishes when it runs again.

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::Store;
use super::files::sync_directory;
use crate::archive::rename_archive;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RenameReport {
    /// The folder got its new name in this run.
    pub moved: bool,
    /// Backups that got the new job name in this run.
    pub recipes: usize,
}

/// Moves the store at `from` to `to` and renames the backups of job `old` in
/// it to `new`. `from` and `to` are the same folder for a store that several
/// jobs share, then only the recipes change.
pub fn rename_job(from: &Path, to: &Path, old: &str, new: &str) -> Result<RenameReport> {
    let moved = move_folder(from, to)?;
    let store = Store::open(to)?;
    let mut recipes = 0;
    for name in store.recipe_names()? {
        let Some(renamed) = rename_archive(&name, old, new) else {
            continue;
        };
        let mut recipe = store.read_recipe(&name)?;
        recipe.name = renamed;
        recipe.job = new.to_owned();
        // The new recipe is written before the old one goes, so a stop between
        // the 2 leaves the backup under both names and never under none.
        store.write_recipe(&recipe)?;
        store.remove_recipe(&name)?;
        recipes += 1;
    }
    Ok(RenameReport { moved, recipes })
}

fn move_folder(from: &Path, to: &Path) -> Result<bool> {
    if from == to {
        return Ok(false);
    }
    if !exists(from)? {
        if exists(to)? {
            return Ok(false);
        }
        bail!("neither {} nor {} exists", from.display(), to.display());
    }
    if exists(to)? {
        // A store opened under the new name before the rename, by a listing for
        // example, leaves empty folders. Anything more is a store of its own.
        if holds_files(to)? {
            bail!(
                "{} and {} both hold files, a rename does not merge 2 stores",
                from.display(),
                to.display()
            );
        }
        remove_empty(to)?;
    }
    fs::rename(from, to)
        .with_context(|| format!("rename {} to {}", from.display(), to.display()))?;
    sync_directory(to.parent().context("a destination has no parent folder")?)?;
    Ok(true)
}

fn exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn holds_files(path: &Path) -> Result<bool> {
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read {}", path.display()))?;
        let kind = entry
            .file_type()
            .with_context(|| format!("read {}", entry.path().display()))?;
        if !kind.is_dir() || holds_files(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remove_empty(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read {}", path.display()))?;
        remove_empty(&entry.path())?;
    }
    fs::remove_dir(path).with_context(|| format!("remove the empty folder {}", path.display()))
}
