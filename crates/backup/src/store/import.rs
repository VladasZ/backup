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
use tracing::warn;

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

/// Imports one old archive into every folder in `roots` that holds a copy
/// of it. The archive is read once, from the first copy that matches its
/// checksum file, and the chunks go to every store at once. Each store is then
/// rebuilt and checked on its own before that folder's original is deleted.
pub fn import_group(roots: &[PathBuf], name: &str) -> Result<ImportReport> {
    parse_archive_name(name).with_context(|| format!("{name} is not an archive name"))?;
    let holders: Vec<PathBuf> = roots
        .iter()
        .filter(|root| root.join(name).exists())
        .cloned()
        .collect();
    if holders.is_empty() {
        bail!("no folder holds {name}");
    }
    let mut failures = Vec::new();
    for source in &holders {
        match import_from(source, &holders, name) {
            Ok(report) => return Ok(report),
            Err(error) => {
                warn!(archive = name, source = %source.display(), error = %format!("{error:#}"), "could not import from this copy; trying the next one");
                failures.push(format!("{}: {error:#}", source.display()));
            }
        }
    }
    bail!("every copy of {name} failed: {}", failures.join("; "))
}

fn import_from(source: &Path, roots: &[PathBuf], name: &str) -> Result<ImportReport> {
    let parsed =
        parse_archive_name(name).with_context(|| format!("{name} is not an archive name"))?;
    let path = source.join(name);
    let source_checksum = checksum_path(&path);
    let expected = if source_checksum.exists() {
        Some(read_checksum(&source_checksum)?)
    } else {
        None
    };
    let mut writers = roots
        .iter()
        .map(|root| Store::open(root).map(Store::into_writer))
        .collect::<Result<Vec<_>>>()?;
    let mut chunks = Vec::new();
    let stream = read_original(&path, expected.as_deref(), &mut |bytes| {
        let id = Digest::of(bytes);
        chunks.push(RecipeChunk {
            id,
            size: u32::try_from(bytes.len()).map_err(io::Error::other)?,
        });
        for writer in &mut writers {
            if !writer.contains(&id).map_err(io::Error::other)? {
                writer.put(&id, bytes).map_err(io::Error::other)?;
            }
        }
        Ok(())
    })?;
    let recipe = Recipe {
        job: parsed.job.to_owned(),
        name: name.to_owned(),
        created: parsed.created,
        size: stream.size,
        checksum: stream.checksum,
        chunks,
    };
    for writer in writers {
        let mut store = writer.finish()?;
        if store.has_recipe(name) {
            // A crash after the recipe was written but before the original
            // was deleted. The recipe must still describe this archive.
            let existing = store.read_recipe(name)?;
            if existing.checksum != recipe.checksum || existing.size != recipe.size {
                bail!(
                    "the existing recipe of {name} in {} does not match the original archive",
                    store.root().display()
                );
            }
        }
        {
            let mut sources: [SourceRef<'_>; 1] = [&mut store];
            let mut rebuilt = RecipeStream::new(&mut sources, &recipe);
            verify_stream(&mut rebuilt).with_context(|| {
                format!(
                    "check the imported copy of {name} in {}",
                    store.root().display()
                )
            })?;
        }
        if !store.has_recipe(name) {
            store.write_recipe(&recipe)?;
        }
        let original = store.root().join(name);
        remove_if_present(&original)?;
        remove_if_present(&checksum_path(&original))?;
        sync_directory(store.root())?;
    }
    Ok(ImportReport {
        name: name.to_owned(),
        size: recipe.size,
    })
}

fn checksum_path(archive: &Path) -> PathBuf {
    PathBuf::from(format!("{}.blake3", archive.display()))
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
