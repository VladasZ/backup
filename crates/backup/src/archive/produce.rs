use std::collections::HashSet;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use tracing::warn;

use super::catalog::{SourceScanner, Visit, changed_paths};
use super::format::ArchiveWriter;
use crate::chunking::Chunker;
use crate::store::digest::Digest;
use crate::store::recipe::RecipeChunk;

const CHANGED_PATHS_IN_LOG: usize = 20;

/// One chunk of the tar stream. A chunk every destination already holds is
/// sent as a reference, so only new data moves.
pub enum ChunkEvent<'bytes> {
    Data { id: Digest, bytes: &'bytes [u8] },
    Reference { id: Digest, size: u32 },
}

impl ChunkEvent<'_> {
    pub fn id(&self) -> Digest {
        match self {
            Self::Data { id, .. } | Self::Reference { id, .. } => *id,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::Data { bytes, .. } => bytes.len() as u64,
            Self::Reference { size, .. } => u64::from(*size),
        }
    }
}

#[derive(Debug)]
pub struct Produced {
    pub chunks: Vec<RecipeChunk>,
    pub size: u64,
    pub checksum: String,
    pub changed: Vec<PathBuf>,
}

/// Writes the source as a tar stream, cuts it into chunks, and hands each
/// chunk to `emit`. `known` holds the chunks every destination has, and grows
/// with every chunk sent, so a repeat inside one stream is a reference too.
pub fn produce(
    job: &str,
    scanner: &SourceScanner,
    known: &mut HashSet<Digest>,
    emit: &mut dyn FnMut(ChunkEvent<'_>) -> Result<()>,
) -> Result<Produced> {
    let mut chunks = Vec::new();
    let mut on_chunk = |bytes: &[u8]| -> io::Result<()> {
        let id = Digest::of(bytes);
        let size = u32::try_from(bytes.len()).map_err(io::Error::other)?;
        chunks.push(RecipeChunk { id, size });
        let event = if known.insert(id) {
            ChunkEvent::Data { id, bytes }
        } else {
            ChunkEvent::Reference { id, size }
        };
        emit(event).map_err(|error| io::Error::other(format!("{error:#}")))
    };
    let (before, stream) = {
        let mut writer = ArchiveWriter::new(Chunker::new(&mut on_chunk));
        let before = scanner.walk(&mut |entry| writer.append(entry))?;
        (before, writer.finish()?.finish()?)
    };
    for mount in &before.skipped_mounts {
        warn!(job, path = %mount.display(), "skipping nested mount");
    }
    for special in &before.skipped_special {
        warn!(job, path = %special.display(), "skipping special file");
    }
    for unreadable in &before.skipped_unreadable {
        warn!(job, path = %unreadable.path.display(), reason = unreadable.reason, "skipping unreadable entry");
    }
    let changed = match scanner.walk(&mut |_| Ok(Visit::Stored)) {
        Ok(after) => changed_paths(&before.fingerprints, &after.fingerprints),
        Err(error) => {
            warn!(job, %error, "could not rescan the source after archiving; consistency is unknown");
            Vec::new()
        }
    };
    warn_changed(job, &changed);
    Ok(Produced {
        chunks,
        size: stream.size,
        checksum: stream.checksum,
        changed,
    })
}

pub fn warn_changed(job: &str, changed: &[PathBuf]) {
    if changed.is_empty() {
        return;
    }
    let shown: Vec<String> = changed
        .iter()
        .take(CHANGED_PATHS_IN_LOG)
        .map(|path| path.display().to_string())
        .collect();
    warn!(
        job,
        count = changed.len(),
        paths = ?shown,
        "source changed while it was being archived; the archive may be inconsistent"
    );
}
