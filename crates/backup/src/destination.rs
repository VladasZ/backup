//! Operations on one destination, local or over SSH. A remote destination
//! runs the same store code inside its agent.

use std::collections::HashSet;
use std::io::Read;

use anyhow::{Context, Result};

use crate::archive::verify_stream;
use crate::config::BackupJob;
use crate::location::Location;
use crate::ssh::{self, RemoteSource, SshRepair};
use crate::store::check::{StoreCheck, check_store, finish_repair, put_repaired};
use crate::store::digest::Digest;
use crate::store::gc::apply_retention;
use crate::store::import::{ImportReport, import_group, legacy_names};
use crate::store::recipe::{Recipe, RecipeInfo};
use crate::store::{Store, StoreWriter};
use crate::stream::{ChunkSource, RecipeStream, SourceRef};

pub fn list(destination: &Location, job: Option<&str>) -> Result<Vec<RecipeInfo>> {
    match destination {
        Location::Local(path) => Store::open(path)?.list_recipes(job),
        Location::Ssh(remote) => ssh::list(remote, job),
    }
}

pub fn chunk_ids(destination: &Location) -> Result<HashSet<Digest>> {
    match destination {
        Location::Local(path) => Store::open(path)?.chunk_ids(),
        Location::Ssh(remote) => ssh::chunk_ids(remote),
    }
}

pub fn read_recipe(destination: &Location, name: &str) -> Result<Recipe> {
    match destination {
        Location::Local(path) => Store::open(path)?.read_recipe(name),
        Location::Ssh(remote) => ssh::read_recipe(remote, name),
    }
}

/// A chunk source for a destination. A remote one streams `order` over one
/// connection when the chunks are then read in that order.
pub fn open_source(destination: &Location, order: &[Digest]) -> Result<Box<dyn ChunkSource>> {
    match destination {
        Location::Local(path) => Ok(Box::new(Store::open(path)?)),
        Location::Ssh(remote) => {
            let mut source = RemoteSource::new(remote);
            source.prefetch(order)?;
            Ok(Box::new(source))
        }
    }
}

pub fn check(destination: &Location) -> Result<StoreCheck> {
    match destination {
        Location::Local(path) => check_store(&mut Store::open(path)?),
        Location::Ssh(remote) => ssh::check(remote),
    }
}

/// Takes good copies of chunks a check found bad, then retires the damaged
/// packs and writes back lost recipes.
pub enum Repair {
    Local {
        writer: Box<StoreWriter>,
        damaged: Vec<Digest>,
        recipes: Vec<Recipe>,
    },
    Remote(SshRepair),
}

impl Repair {
    pub fn open(destination: &Location, damaged: &[Digest], recipes: &[Recipe]) -> Result<Self> {
        Ok(match destination {
            Location::Local(path) => Self::Local {
                writer: Box::new(Store::open(path)?.into_writer()),
                damaged: damaged.to_vec(),
                recipes: recipes.to_vec(),
            },
            Location::Ssh(remote) => Self::Remote(SshRepair::open(remote, damaged, recipes)?),
        })
    }

    pub fn put(&mut self, id: &Digest, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Local { writer, .. } => put_repaired(writer, id, bytes),
            Self::Remote(repair) => repair.put(id, bytes),
        }
    }

    pub fn finish(self) -> Result<()> {
        match self {
            Self::Local {
                writer,
                damaged,
                recipes,
            } => finish_repair((*writer).finish()?, &damaged, &recipes).map(drop),
            Self::Remote(repair) => repair.finish(),
        }
    }
}

/// Rebuilds one backup from this destination alone and reads it as a tar.
pub fn verify_archive(destination: &Location, name: &str) -> Result<()> {
    match destination {
        Location::Local(path) => {
            let mut store = Store::open(path)?;
            let recipe = store.read_recipe(name)?;
            verify_local(&mut store, &recipe)
        }
        Location::Ssh(remote) => ssh::verify_archive(remote, name),
    }
}

pub fn verify_local(store: &mut Store, recipe: &Recipe) -> Result<()> {
    let mut sources: [SourceRef<'_>; 1] = [store];
    let mut stream = RecipeStream::new(&mut sources, recipe);
    verify_stream(&mut stream as &mut dyn Read)
}

pub fn prune(destination: &Location, job: &BackupJob) -> Result<()> {
    match destination {
        Location::Local(path) => apply_retention(Store::open(path)?, job).map(drop),
        Location::Ssh(remote) => ssh::prune(remote, job),
    }
}

pub fn legacy(destination: &Location) -> Result<Vec<String>> {
    match destination {
        Location::Local(path) => legacy_names(path),
        Location::Ssh(remote) => ssh::legacy(remote),
    }
}

/// Imports one old archive into every destination that holds it. The local
/// ones share a single read of the archive. A remote one imports its own copy
/// inside its agent.
pub fn import(destinations: &[Location], name: &str) -> Result<ImportReport> {
    let local: Vec<_> = destinations
        .iter()
        .filter_map(|destination| match destination {
            Location::Local(path) => Some(path.clone()),
            Location::Ssh(_) => None,
        })
        .collect();
    let mut report = None;
    if !local.is_empty() {
        report = Some(import_group(&local, name)?);
    }
    for destination in destinations {
        if let Location::Ssh(remote) = destination {
            report = Some(ssh::import(remote, name)?);
        }
    }
    report.context("no destination to import into")
}
