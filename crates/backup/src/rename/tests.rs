use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::slice::from_ref;

use chrono::{Duration, Utc};
use tempfile::{TempDir, tempdir};

use super::rename;
use crate::archive::{Artifact, ChunkEvent, SourceScanner, archive_name, produce};
use crate::config::{BackupJob, Config};
use crate::destination::verify_archive;
use crate::location::Location;
use crate::state::{DeliveryResult, DeliveryStatus, State};
use crate::store::Store;
use crate::store::recipe::Recipe;

struct Setup {
    temporary: TempDir,
    source: PathBuf,
    state: State,
}

impl Setup {
    fn new() -> Self {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("source");
        fs::create_dir_all(source.join("worlds")).unwrap();
        fs::write(source.join("worlds/world.db"), vec![7u8; 300_000]).unwrap();
        fs::write(source.join("adminlist.txt"), "admin").unwrap();
        let state = State::open(&temporary.path().join("state.redb")).unwrap();
        Self {
            temporary,
            source,
            state,
        }
    }

    fn folder(&self, name: &str) -> PathBuf {
        self.temporary.path().join("backups").join(name)
    }

    fn job(&self, name: &str, destinations: &[PathBuf]) -> BackupJob {
        BackupJob {
            name: name.to_owned(),
            source: Location::Local(self.source.clone()),
            destinations: destinations
                .iter()
                .map(|path| Location::Local(path.clone()))
                .collect(),
            cron: "0 3 * * *".to_owned(),
            retention: None,
            pre: None,
            exclude: Vec::new(),
        }
    }

    /// A backup of the source into `root` the way a run makes one, recorded
    /// in the state as delivered when `delivered` is set.
    fn backup(&self, root: &Path, job: &BackupJob, delivered: bool) -> Recipe {
        let scanner = SourceScanner::new(&self.source, &[]).unwrap();
        let mut writer = Store::open(root).unwrap().into_writer();
        let mut known = HashSet::new();
        let produced = produce(&job.name, &scanner, &mut known, &mut |event| {
            if let ChunkEvent::Data { id, bytes } = event
                && !writer.contains(&id)?
            {
                writer.put(&id, bytes)?;
            }
            Ok(())
        })
        .unwrap();
        let mut store = writer.finish().unwrap();
        let created = Utc::now();
        let recipe = Recipe {
            job: job.name.clone(),
            name: archive_name(&job.name, created),
            created,
            size: produced.size,
            checksum: produced.checksum,
            chunks: produced.chunks,
        };
        store.ensure_complete(&recipe).unwrap();
        store.write_recipe(&recipe).unwrap();
        let artifact = Artifact {
            name: recipe.name.clone(),
            checksum: recipe.checksum.clone(),
            size: recipe.size,
            created_at: created,
        };
        let results: Vec<DeliveryResult> = if delivered {
            job.destinations
                .iter()
                .map(|destination| DeliveryResult {
                    destination: destination.clone(),
                    status: DeliveryStatus::Delivered,
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut state = State::open(&self.temporary.path().join("state.redb")).unwrap();
        let run = state.register_run(&artifact, job, &results).unwrap();
        if delivered {
            state.complete_ready_runs().unwrap();
            state.mark_run_complete(run).unwrap();
        }
        recipe
    }
}

fn config(jobs: Vec<BackupJob>) -> Config {
    Config { jobs }
}

fn names(root: &Path, job: &str) -> Vec<String> {
    Store::open(root)
        .unwrap()
        .list_recipes(Some(job))
        .unwrap()
        .into_iter()
        .map(|info| info.name)
        .collect()
}

#[test]
fn a_rename_moves_the_store_and_its_backups_still_restore() {
    let setup = Setup::new();
    let old_folder = setup.folder("valheim-hard");
    let job = setup.job("valheim-hard", from_ref(&old_folder));
    let first = setup.backup(&old_folder, &job, true);
    let second = setup.backup(&old_folder, &job, true);
    let slot = Utc::now() - Duration::hours(1);
    setup
        .state
        .set_last_scheduled("valheim-hard", slot)
        .unwrap();
    setup
        .state
        .set_last_verified(&job.destinations[0], slot)
        .unwrap();

    let report = rename(
        &config(vec![job.clone()]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();

    let new_folder = setup.folder("arkham-asylum");
    assert!(!old_folder.exists());
    assert_eq!(report.destinations.len(), 1);
    assert!(report.destinations[0].moved);
    assert_eq!(report.destinations[0].recipes, 2);
    assert!(names(&new_folder, "valheim-hard").is_empty());
    let renamed = names(&new_folder, "arkham-asylum");
    assert_eq!(renamed.len(), 2);
    for recipe in [&first, &second] {
        let name = recipe.name.replacen("valheim-hard", "arkham-asylum", 1);
        assert!(renamed.contains(&name), "{name} is missing");
        let stored = Store::open(&new_folder)
            .unwrap()
            .read_recipe(&name)
            .unwrap();
        assert_eq!(stored.job, "arkham-asylum");
        assert_eq!(stored.checksum, recipe.checksum);
        verify_archive(&Location::Local(new_folder.clone()), &name).unwrap();
    }

    let history = setup.state.history().unwrap();
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|line| line.job == "arkham-asylum"));
    assert!(
        history
            .iter()
            .all(|line| line.archive.starts_with("arkham-asylum-"))
    );
    // Copied, not moved: the daemon still runs the old job until its
    // configuration names the new one, and neither may look due.
    assert_eq!(
        setup.state.last_scheduled("arkham-asylum").unwrap(),
        setup.state.last_scheduled("valheim-hard").unwrap()
    );
    assert!(
        setup
            .state
            .last_scheduled("valheim-hard")
            .unwrap()
            .is_some()
    );
    let new_location = Location::Local(new_folder);
    assert!(setup.state.last_verified(&new_location).unwrap().is_some());
    assert!(
        setup
            .state
            .last_verified(&job.destinations[0])
            .unwrap()
            .is_some()
    );
}

#[test]
fn a_second_run_and_a_run_after_the_config_changed_change_nothing() {
    let setup = Setup::new();
    let old_folder = setup.folder("valheim-hard");
    let new_folder = setup.folder("arkham-asylum");
    let old_job = setup.job("valheim-hard", from_ref(&old_folder));
    setup.backup(&old_folder, &old_job, true);
    rename(
        &config(vec![old_job.clone()]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();

    let again = rename(
        &config(vec![old_job]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();
    assert!(!again.destinations[0].moved);
    assert_eq!(again.destinations[0].recipes, 0);

    let new_job = setup.job("arkham-asylum", from_ref(&new_folder));
    let after = rename(
        &config(vec![new_job]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();
    assert_eq!(after.destinations[0].from, Location::Local(old_folder));
    assert!(!after.destinations[0].moved);
    assert_eq!(names(&new_folder, "arkham-asylum").len(), 1);
}

#[test]
fn a_shared_store_keeps_its_folder_and_the_backups_of_other_jobs() {
    let setup = Setup::new();
    let shared = setup.folder("shared");
    let job = setup.job("valheim-hard", from_ref(&shared));
    let other = setup.job("valheim", from_ref(&shared));
    setup.backup(&shared, &job, true);
    setup.backup(&shared, &other, true);

    let report = rename(
        &config(vec![job, other]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();

    assert!(shared.exists());
    assert!(!report.destinations[0].moved);
    assert_eq!(names(&shared, "arkham-asylum").len(), 1);
    assert_eq!(names(&shared, "valheim").len(), 1);
    assert!(names(&shared, "valheim-hard").is_empty());
}

#[test]
fn a_rename_never_merges_two_stores() {
    let setup = Setup::new();
    let old_folder = setup.folder("valheim-hard");
    let new_folder = setup.folder("arkham-asylum");
    let old_job = setup.job("valheim-hard", from_ref(&old_folder));
    let new_job = setup.job("arkham-asylum", from_ref(&new_folder));
    setup.backup(&old_folder, &old_job, true);
    setup.backup(&new_folder, &new_job, true);

    let error = rename(
        &config(vec![old_job]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("does not merge"), "{error:#}");
    assert_eq!(names(&old_folder, "valheim-hard").len(), 1);
    assert_eq!(names(&new_folder, "arkham-asylum").len(), 1);
}

/// A listing under the new name before the rename opens an empty store there.
#[test]
fn empty_folders_under_the_new_name_give_way() {
    let setup = Setup::new();
    let old_folder = setup.folder("valheim-hard");
    let new_folder = setup.folder("arkham-asylum");
    let job = setup.job("valheim-hard", from_ref(&old_folder));
    setup.backup(&old_folder, &job, true);
    fs::create_dir_all(new_folder.join("recipes")).unwrap();

    let report = rename(
        &config(vec![job]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap();

    assert!(report.destinations[0].moved);
    assert_eq!(names(&new_folder, "arkham-asylum").len(), 1);
}

#[test]
fn a_waiting_delivery_stops_the_rename_before_any_change() {
    let setup = Setup::new();
    let old_folder = setup.folder("valheim-hard");
    let job = setup.job("valheim-hard", from_ref(&old_folder));
    setup.backup(&old_folder, &job, false);

    let error = rename(
        &config(vec![job]),
        &setup.state,
        "valheim-hard",
        "arkham-asylum",
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("still waiting"), "{error:#}");
    assert!(old_folder.exists());
    assert!(!setup.folder("arkham-asylum").exists());
}
