//! Moves archives from the old format, one `.tar.lz4` file per backup, into
//! the store. The original is deleted only after the stream rebuilt from the
//! store matches the original's blake3 byte for byte and reads as a valid tar.

use std::fs::File;
use std::io::{self, BufReader, Read, copy};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use chrono::{DateTime, Utc};
use lz4_flex::frame::FrameDecoder;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::Store;
use super::digest::Digest;
use super::files::{list, remove_if_present, sync_directory};
use super::recipe::{Recipe, RecipeChunk};
use crate::archive::{ARCHIVE_SUFFIX, parse_archive_name, read_checksum, verify_stream};
use crate::chunking::Chunker;
use crate::stream::{RecipeStream, SourceRef};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ImportReport {
    pub name: String,
    pub size: u64,
}

/// Old-format archives in a destination folder, oldest first.
pub fn legacy_names(root: &Path) -> Result<Vec<String>> {
    let mut names: Vec<(DateTime<Utc>, String)> = list(root, ARCHIVE_SUFFIX)?
        .into_iter()
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?.to_owned();
            let created = parse_archive_name(&name)?.created;
            Some((created, name))
        })
        .collect();
    names.sort();
    Ok(names.into_iter().map(|(_, name)| name).collect())
}

pub fn import_legacy(store: Store, name: &str) -> Result<(Store, ImportReport)> {
    let parsed =
        parse_archive_name(name).with_context(|| format!("{name} is not an archive name"))?;
    let job = parsed.job.to_owned();
    let created = parsed.created;
    let path = store.root().join(name);
    let checksum_path = PathBuf::from(format!("{}.blake3", path.display()));
    let expected = if checksum_path.exists() {
        Some(read_checksum(&checksum_path)?)
    } else {
        None
    };

    let (mut store, recipe) = if store.has_recipe(name) {
        // A crash after the recipe was written but before the original was
        // deleted. The recipe is still checked against the original.
        let recipe = store.read_recipe(name)?;
        let stream = read_original(&path, expected.as_deref(), &mut |_| Ok(()))?;
        if stream.checksum != recipe.checksum || stream.size != recipe.size {
            bail!("the existing recipe of {name} does not match the original archive");
        }
        (store, recipe)
    } else {
        let mut writer = store.into_writer();
        let mut chunks = Vec::new();
        let stream = read_original(&path, expected.as_deref(), &mut |bytes| {
            let id = Digest::of(bytes);
            chunks.push(RecipeChunk {
                id,
                size: u32::try_from(bytes.len()).map_err(io::Error::other)?,
            });
            if !writer.contains(&id).map_err(io::Error::other)? {
                writer.put(&id, bytes).map_err(io::Error::other)?;
            }
            Ok(())
        })?;
        let store = writer.finish()?;
        let recipe = Recipe {
            job,
            name: name.to_owned(),
            created,
            size: stream.size,
            checksum: stream.checksum,
            chunks,
        };
        (store, recipe)
    };

    {
        let mut sources: [SourceRef<'_>; 1] = [&mut store];
        let mut rebuilt = RecipeStream::new(&mut sources, &recipe);
        verify_stream(&mut rebuilt)
            .with_context(|| format!("check the imported copy of {name}"))?;
    }
    if !store.has_recipe(name) {
        store.write_recipe(&recipe)?;
    }
    remove_if_present(&path)?;
    remove_if_present(&checksum_path)?;
    sync_directory(store.root())?;
    info!(archive = name, destination = %store.root().display(), "imported an old archive");
    Ok((
        store,
        ImportReport {
            name: name.to_owned(),
            size: recipe.size,
        },
    ))
}

struct OriginalStream {
    checksum: String,
    size: u64,
}

// One pass reads the file, checks it against its checksum file, decompresses
// it, and hands the tar stream to the chunker.
fn read_original(
    path: &Path,
    expected: Option<&str>,
    emit: &mut dyn FnMut(&[u8]) -> io::Result<()>,
) -> Result<OriginalStream> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hashing = HashingReader {
        inner: BufReader::new(file),
        hasher: Hasher::new(),
    };
    let mut chunker = Chunker::new(emit);
    copy(&mut FrameDecoder::new(&mut hashing), &mut chunker)
        .with_context(|| format!("read {}", path.display()))?;
    let stream = chunker.finish()?;
    // The decoder can stop at the end of the frame, so any trailing bytes are
    // still read into the file hash.
    copy(&mut hashing, &mut io::sink())?;
    let actual = hashing.hasher.finalize().to_hex().to_string();
    if let Some(expected) = expected
        && actual != expected
    {
        bail!(
            "{} does not match its checksum file: expected {expected}, got {actual}",
            path.display()
        );
    }
    Ok(OriginalStream {
        checksum: stream.checksum,
        size: stream.size,
    })
}

struct HashingReader<R: Read> {
    inner: R,
    hasher: Hasher,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}
