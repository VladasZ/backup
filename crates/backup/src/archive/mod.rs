mod catalog;
mod checksum;
mod format;
mod produce;
mod restore;

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::ARCHIVE_EXTENSION;

pub use catalog::{SourceScanner, ensure_not_symlink};
pub use checksum::read_checksum;
pub use produce::{ChunkEvent, Produced, produce, warn_changed};
pub use restore::{restore_stream, verify_stream};

pub const ARCHIVE_SUFFIX: &str = ".tar.lz4";

// Archive names are "<job>-<compact UTC seconds>-<uuid>.tar.lz4". Both trailing parts have a
// fixed width, so the job name is whatever is left after removing them, even when it contains a
// dash.
const UUID_LEN: usize = 36;
const TIMESTAMP_LEN: usize = 16;

/// One finished backup as the state database records it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Artifact {
    pub name: String,
    pub checksum: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

// Backups can hold private data, so store files never get the default umask
// where another user of a shared destination could read them. Renames keep the
// mode, so a published file stays 0600.
pub fn create_private(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}

// Colons are illegal in SMB names, so an RFC3339 timestamp makes samba serve
// the file under a mangled 8.3 name. The compact form has no colons.
//
// The name keeps its old `.tar.lz4` ending although a backup is now a recipe:
// `export` turns it into exactly that file, and callers that validate archive
// names by that ending keep working.
pub fn archive_name(job: &str, created_at: DateTime<Utc>) -> String {
    let timestamp = created_at.format("%Y%m%dT%H%M%SZ");
    format!(
        "{job}-{timestamp}-{}.{ARCHIVE_EXTENSION}",
        uuid::Uuid::new_v4()
    )
}

pub struct ParsedArchive<'name> {
    pub job: &'name str,
    pub created: DateTime<Utc>,
}

// A name is also a file name inside a destination, so one that could point
// outside its folder is not an archive name at all.
pub fn parse_archive_name(name: &str) -> Option<ParsedArchive<'_>> {
    if name.contains('/') || name.starts_with('.') {
        return None;
    }
    let rest = name.strip_suffix(ARCHIVE_SUFFIX)?;
    let rest = rest.get(..rest.len().checked_sub(UUID_LEN + 1)?)?;
    let split = rest.len().checked_sub(TIMESTAMP_LEN + 1)?;
    let job = rest.get(..split)?;
    let stamp = rest.get(split..)?.strip_prefix('-')?;
    let created = NaiveDateTime::parse_from_str(stamp, "%Y%m%dT%H%M%SZ")
        .ok()?
        .and_utc();
    Some(ParsedArchive { job, created })
}

#[cfg(test)]
mod tests {
    use super::parse_archive_name;

    #[test]
    fn a_job_name_containing_dashes_still_parses() {
        let name = "my-nice-job-20260717T020000Z-11111111-1111-1111-1111-111111111111.tar.lz4";
        let parsed = parse_archive_name(name).unwrap();
        assert_eq!(parsed.job, "my-nice-job");
        assert_eq!(parsed.created.to_rfc3339(), "2026-07-17T02:00:00+00:00");
        assert!(parse_archive_name("not-an-archive.txt").is_none());
        let escaping = "../../etc/x-20260717T020000Z-11111111-1111-1111-1111-111111111111.tar.lz4";
        assert!(parse_archive_name(escaping).is_none());
    }
}
