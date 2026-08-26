use std::path::PathBuf;

use chrono::{Duration, Timelike, Utc};
use tempfile::tempdir;

use super::State;
use crate::archive::Artifact;
use crate::config::BackupJob;
use crate::location::Location;

#[test]
fn tracks_each_destination_and_completes_only_after_all_deliveries() {
    let temporary = tempdir().unwrap();
    let mut state = State::open(&temporary.path().join("state.redb")).unwrap();
    let first = Location::Local(PathBuf::from("/first"));
    let second = Location::Local(PathBuf::from("/second"));
    let job = BackupJob {
        name: "documents".to_owned(),
        source: Location::Local(PathBuf::from("/source")),
        destinations: vec![first.clone(), second.clone()],
        cron: "0 2 * * *".to_owned(),
        retention: None,
        exclude: Vec::new(),
    };
    let artifact = Artifact {
        name: "documents-archive.tar".to_owned(),
        path: temporary.path().join("archive.tar"),
        checksum_path: temporary.path().join("archive.tar.blake3"),
        checksum: "abc".to_owned(),
        size: 12,
        created_at: Utc::now(),
    };
    let run_id = state.register_run(&artifact, &job).unwrap();
    let due = state.due_deliveries(Utc::now()).unwrap();
    assert_eq!(due.len(), 2);

    state.mark_delivered(run_id, &first).unwrap();
    assert!(state.complete_ready_runs().unwrap().is_empty());
    let retry_at = state
        .mark_delivery_failed(run_id, &second, 0, "offline")
        .unwrap();
    assert!(
        state
            .due_deliveries(retry_at - Duration::seconds(1))
            .unwrap()
            .is_empty()
    );
    let retry = state.due_deliveries(retry_at).unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].destination, second);
    assert_eq!(retry[0].attempts, 1);

    state.mark_delivered(run_id, &second).unwrap();
    let ready = state.complete_ready_runs().unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].run_id, run_id);
    state.mark_run_complete(run_id).unwrap();
    assert!(state.status().unwrap().is_empty());
}

#[test]
fn scheduled_backup_failure_grows_the_retry_delay() {
    let temporary = tempdir().unwrap();
    let state = State::open(&temporary.path().join("state.redb")).unwrap();
    let slot = Utc::now()
        .with_nanosecond(0)
        .expect("valid timestamp without nanoseconds");

    assert!(state.backup_retry("documents").unwrap().is_none());

    let first = state.record_backup_failure("documents", slot, 0).unwrap();
    let recorded = state.backup_retry("documents").unwrap().unwrap();
    assert_eq!(recorded.attempts, 1);
    assert_eq!(recorded.slot, slot);

    let second = state
        .record_backup_failure("documents", slot, recorded.attempts)
        .unwrap();
    assert!(second > first);
    assert_eq!(
        state.backup_retry("documents").unwrap().unwrap().attempts,
        2
    );

    state.clear_backup_retry("documents").unwrap();
    assert!(state.backup_retry("documents").unwrap().is_none());
}

#[test]
fn stores_last_handled_schedule_in_utc() {
    let temporary = tempdir().unwrap();
    let state = State::open(&temporary.path().join("state.redb")).unwrap();
    let scheduled = Utc::now()
        .with_nanosecond(0)
        .expect("valid timestamp without nanoseconds");
    state.set_last_scheduled("documents", scheduled).unwrap();
    assert_eq!(state.last_scheduled("documents").unwrap(), Some(scheduled));
}
