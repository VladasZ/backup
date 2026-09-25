use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use chrono::{Duration as ChronoDuration, Utc};
use lz4_flex::frame::FrameEncoder;
use tempfile::tempdir;

use super::Store;
use super::check::{check_store, finish_repair, put_repaired};
use super::digest::Digest;
use super::gc::{apply_retention, collect_garbage};
use super::import::{import_legacy, legacy_names};
use super::index::{INDEX, pack_files};
use super::recipe::Recipe;
use crate::archive::{ChunkEvent, SourceScanner, archive_name, produce, restore_stream};
use crate::config::{BackupJob, RetentionConfig};
use crate::location::Location;
use crate::stream::{RecipeStream, SourceRef};

fn job(name: &str, retention: Option<usize>) -> BackupJob {
    BackupJob {
        name: name.to_owned(),
        source: Location::Local(PathBuf::from("/unused")),
        destinations: vec![Location::Local(PathBuf::from("/unused"))],
        cron: "0 0 * * *".to_owned(),
        retention: retention.map(|count| RetentionConfig {
            count: Some(count),
            age: None,
        }),
        pre: None,
        exclude: Vec::new(),
    }
}

fn noise(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// Backs up `source` into `store` the way a run does, and returns the recipe.
fn backup(store: Store, source: &Path, job: &str) -> (Store, Recipe) {
    let scanner = SourceScanner::new(source, &[]).unwrap();
    let mut writer = store.into_writer();
    let mut known: HashSet<_> = HashSet::new();
    let produced = produce(job, &scanner, &mut known, &mut |event| {
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
        job: job.to_owned(),
        name: archive_name(job, created),
        created,
        size: produced.size,
        checksum: produced.checksum,
        chunks: produced.chunks,
    };
    store.ensure_complete(&recipe).unwrap();
    store.write_recipe(&recipe).unwrap();
    (store, recipe)
}

fn restore(store: &mut Store, recipe: &Recipe, target: &Path) {
    let mut sources: [SourceRef<'_>; 1] = [store];
    let mut stream = RecipeStream::new(&mut sources, recipe);
    restore_stream(&mut stream as &mut dyn Read, target).unwrap();
}

fn stored_bytes(root: &Path) -> u64 {
    pack_files(root)
        .unwrap()
        .iter()
        .map(|(_, path)| fs::metadata(path).unwrap().len())
        .sum()
}

fn age_packs(root: &Path) {
    let old = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
    for (_, path) in pack_files(root).unwrap() {
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
    }
}

fn source_with(root: &Path, files: &[(&str, Vec<u8>)]) -> PathBuf {
    let source = root.join("source");
    fs::create_dir_all(&source).unwrap();
    for (name, bytes) in files {
        fs::write(source.join(name), bytes).unwrap();
    }
    source
}

#[test]
fn a_backup_restores_byte_for_byte() {
    let temporary = tempdir().unwrap();
    let source = source_with(
        temporary.path(),
        &[
            ("big.bin", noise(9 * 1024 * 1024, 1)),
            ("small.txt", b"hello".to_vec()),
        ],
    );
    let (mut store, recipe) = backup(
        Store::open(&temporary.path().join("dest")).unwrap(),
        &source,
        "docs",
    );

    let target = temporary.path().join("restored");
    restore(&mut store, &recipe, &target);

    assert_eq!(
        fs::read(target.join("big.bin")).unwrap(),
        noise(9 * 1024 * 1024, 1)
    );
    assert_eq!(fs::read(target.join("small.txt")).unwrap(), b"hello");
    let listed = store.list_recipes(Some("docs")).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].size, recipe.size);
}

#[test]
fn an_unchanged_source_adds_no_new_data() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("big.bin", noise(12 * 1024 * 1024, 2))]);
    let root = temporary.path().join("dest");
    let (store, _) = backup(Store::open(&root).unwrap(), &source, "docs");
    let first = stored_bytes(&root);
    let (store, _) = backup(store, &source, "docs");
    let (store, _) = backup(store, &source, "docs");

    assert_eq!(stored_bytes(&root), first);
    assert_eq!(store.list_recipes(Some("docs")).unwrap().len(), 3);
}

#[test]
fn a_small_edit_stores_only_the_chunks_around_it() {
    let temporary = tempdir().unwrap();
    let data = noise(24 * 1024 * 1024, 3);
    let source = source_with(temporary.path(), &[("big.bin", data.clone())]);
    let root = temporary.path().join("dest");
    let (store, _) = backup(Store::open(&root).unwrap(), &source, "docs");
    let first = stored_bytes(&root);

    let mut edited = data[..5_000_000].to_vec();
    edited.extend_from_slice(b"a few new bytes in the middle");
    edited.extend_from_slice(&data[5_000_000..]);
    fs::write(source.join("big.bin"), &edited).unwrap();
    let (mut store, recipe) = backup(store, &source, "docs");

    let added = stored_bytes(&root) - first;
    assert!(added < 10 * 1024 * 1024, "an edit stored {added} new bytes");
    let target = temporary.path().join("restored");
    restore(&mut store, &recipe, &target);
    assert_eq!(fs::read(target.join("big.bin")).unwrap(), edited);
}

#[test]
fn retention_removes_recipes_and_cleanup_frees_their_packs() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("a.bin", noise(3 * 1024 * 1024, 4))]);
    let root = temporary.path().join("dest");
    let (store, _) = backup(Store::open(&root).unwrap(), &source, "docs");
    fs::write(source.join("a.bin"), noise(3 * 1024 * 1024, 5)).unwrap();
    let (store, latest) = backup(store, &source, "docs");
    age_packs(&root);

    let mut store = apply_retention(store, &job("docs", Some(1))).unwrap();

    let names = store.recipe_names().unwrap();
    assert_eq!(names, vec![latest.name.clone()]);
    assert!(
        stored_bytes(&root) < 5 * 1024 * 1024,
        "the old data was not freed"
    );
    let target = temporary.path().join("restored");
    restore(&mut store, &latest, &target);
    assert_eq!(
        fs::read(target.join("a.bin")).unwrap(),
        noise(3 * 1024 * 1024, 5)
    );
    assert!(check_store(&mut store).unwrap().is_clean());
}

#[test]
fn cleanup_leaves_young_unreferenced_packs_alone() {
    let temporary = tempdir().unwrap();
    let root = temporary.path().join("dest");
    let mut writer = Store::open(&root).unwrap().into_writer();
    let orphan = noise(1024 * 1024, 6);
    writer.put(&Digest::of(&orphan), &orphan).unwrap();
    let store = writer.finish().unwrap();

    let store = collect_garbage(store).unwrap();
    assert_eq!(
        pack_files(&root).unwrap().len(),
        1,
        "a young pack was removed"
    );

    age_packs(&root);
    collect_garbage(store).unwrap();
    assert!(
        pack_files(&root).unwrap().is_empty(),
        "an old unused pack survived"
    );
}

#[test]
fn parity_heals_a_damaged_pack_without_another_copy() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("a.bin", noise(2 * 1024 * 1024, 7))]);
    let root = temporary.path().join("dest");
    let (store, recipe) = backup(Store::open(&root).unwrap(), &source, "docs");
    drop(store);
    let (_, pack) = pack_files(&root).unwrap().remove(0);
    let mut bytes = fs::read(&pack).unwrap();
    bytes[5000] ^= 0xff;
    fs::write(&pack, &bytes).unwrap();

    let mut store = Store::open(&root).unwrap();
    let check = check_store(&mut store).unwrap();
    assert!(check.is_clean(), "parity did not heal the pack: {check:?}");
    assert_ne!(
        fs::read(&pack).unwrap(),
        bytes,
        "the healed pack was not rewritten"
    );
    let target = temporary.path().join("restored");
    restore(&mut store, &recipe, &target);
}

#[test]
fn damage_beyond_parity_is_repaired_from_a_good_copy() {
    let temporary = tempdir().unwrap();
    let data = noise(2 * 1024 * 1024, 8);
    let source = source_with(temporary.path(), &[("a.bin", data.clone())]);
    let root = temporary.path().join("dest");
    let good_root = temporary.path().join("good");
    let (store, recipe) = backup(Store::open(&root).unwrap(), &source, "docs");
    drop(store);
    let (mut good, _) = backup(Store::open(&good_root).unwrap(), &source, "docs");

    let (_, pack) = pack_files(&root).unwrap().remove(0);
    let mut bytes = fs::read(&pack).unwrap();
    for offset in [3000, 400_000, 900_000, 1_500_000] {
        bytes[offset] ^= 0xff;
    }
    fs::write(&pack, &bytes).unwrap();

    let mut store = Store::open(&root).unwrap();
    let check = check_store(&mut store).unwrap();
    assert!(!check.is_clean());
    assert!(!check.bad_chunks.is_empty());

    let mut writer = store.into_writer();
    for id in &check.bad_chunks {
        put_repaired(&mut writer, id, &good.read_chunk(id).unwrap()).unwrap();
    }
    let mut store = finish_repair(writer.finish().unwrap(), &check.damaged_packs, &[]).unwrap();

    assert!(check_store(&mut store).unwrap().is_clean());
    assert!(!pack.exists(), "the damaged pack was kept");
    let target = temporary.path().join("restored");
    restore(&mut store, &recipe, &target);
    assert_eq!(fs::read(target.join("a.bin")).unwrap(), data);
}

#[test]
fn losing_every_index_file_loses_nothing() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("a.bin", noise(2 * 1024 * 1024, 9))]);
    let root = temporary.path().join("dest");
    let (store, recipe) = backup(Store::open(&root).unwrap(), &source, "docs");
    drop(store);
    fs::remove_dir_all(root.join(INDEX)).unwrap();

    let mut store = Store::open(&root).unwrap();
    restore(&mut store, &recipe, &temporary.path().join("restored"));
    assert!(
        root.join(INDEX).read_dir().unwrap().count() > 0,
        "the index was not rebuilt"
    );
}

#[test]
fn a_run_that_dies_before_its_recipe_publishes_nothing() {
    let temporary = tempdir().unwrap();
    let root = temporary.path().join("dest");
    let mut writer = Store::open(&root).unwrap().into_writer();
    let data = noise(40 * 1024 * 1024, 10);
    for piece in data.chunks(1024 * 1024) {
        writer.put(&Digest::of(piece), piece).unwrap();
    }
    drop(writer);

    let store = Store::open(&root).unwrap();
    assert!(store.list_recipes(None).unwrap().is_empty());
    age_packs(&root);
    collect_garbage(store).unwrap();
    assert!(pack_files(&root).unwrap().is_empty());
}

fn legacy_archive(root: &Path, source: &Path, job: &str, corrupt: bool) -> String {
    let name = archive_name(job, Utc::now() - ChronoDuration::days(3));
    let mut tar = tar::Builder::new(FrameEncoder::new(Vec::new()));
    tar.append_dir_all(".", source).unwrap();
    let bytes = tar.into_inner().unwrap().finish().unwrap();
    fs::create_dir_all(root).unwrap();
    fs::write(root.join(&name), &bytes).unwrap();
    let checksum = if corrupt {
        "0".repeat(64)
    } else {
        blake3::hash(&bytes).to_hex().to_string()
    };
    let mut file = File::create(root.join(format!("{name}.blake3"))).unwrap();
    writeln!(file, "{checksum}  {name}").unwrap();
    name
}

#[test]
fn an_old_archive_is_imported_and_then_deleted() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("a.bin", noise(3 * 1024 * 1024, 11))]);
    let root = temporary.path().join("dest");
    let name = legacy_archive(&root, &source, "docs", false);
    assert_eq!(legacy_names(&root).unwrap(), vec![name.clone()]);

    let (mut store, report) = import_legacy(Store::open(&root).unwrap(), &name).unwrap();

    assert_eq!(report.name, name);
    assert!(!root.join(&name).exists(), "the original was kept");
    assert!(!root.join(format!("{name}.blake3")).exists());
    assert!(legacy_names(&root).unwrap().is_empty());
    let recipe = store.read_recipe(&name).unwrap();
    let target = temporary.path().join("restored");
    restore(&mut store, &recipe, &target);
    assert_eq!(
        fs::read(target.join("a.bin")).unwrap(),
        noise(3 * 1024 * 1024, 11)
    );
}

#[test]
fn an_old_archive_that_fails_its_checksum_is_kept() {
    let temporary = tempdir().unwrap();
    let source = source_with(temporary.path(), &[("a.bin", b"data".to_vec())]);
    let root = temporary.path().join("dest");
    let name = legacy_archive(&root, &source, "docs", true);

    let result = import_legacy(Store::open(&root).unwrap(), &name);

    assert!(result.is_err());
    assert!(
        root.join(&name).exists(),
        "a failed import deleted the original"
    );
    assert!(!Store::open(&root).unwrap().has_recipe(&name));
}
