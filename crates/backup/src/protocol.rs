use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::archive::Artifact;
use crate::config::BackupJob;

pub const RESPONSE_PREFIX: &str = "BACKUP/1 ";
pub const PROTOCOL_VERSION: u32 = 3;
const MAX_FRAME: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AgentRequest {
    Ping,
    ValidateSource {
        path: PathBuf,
    },
    ValidateDestination {
        path: PathBuf,
    },
    Create {
        job: String,
        source: PathBuf,
        exclude: Vec<String>,
        pre: Option<String>,
    },
    Receive {
        name: String,
        destination: PathBuf,
        job: BackupJob,
    },
    List {
        destination: PathBuf,
        job: String,
    },
    Send {
        archive: PathBuf,
        checksum: String,
    },
    Restore {
        artifact: WireArtifact,
        target: PathBuf,
    },
    Verify {
        archive: PathBuf,
        checksum: String,
    },
    Prune {
        destination: PathBuf,
        job: BackupJob,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WireArtifact {
    pub name: String,
    pub checksum: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StreamHeader {
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StreamTrailer {
    pub checksum: String,
    pub size: u64,

    #[serde(default)]
    pub changed: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WireArchiveInfo {
    pub name: String,
    pub checksum: Option<String>,
    pub size: u64,
    pub created: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PingResponse {
    pub protocol: u32,
    pub version: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResponseEnvelope {
    pub ok: bool,
    pub error: Option<String>,

    #[serde(default)]
    pub data: serde_json::Value,

    // Zero when the remote binary predates the field, so a mismatch is
    // reported clearly instead of failing later with a decode error.
    #[serde(default)]
    pub protocol: u32,
}

impl WireArtifact {
    pub fn from_artifact(artifact: &Artifact) -> Self {
        Self {
            name: artifact.name.clone(),
            checksum: artifact.checksum.clone(),
            size: artifact.size,
            created_at: artifact.created_at,
        }
    }
}

pub fn write_frame(writer: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    for chunk in bytes.chunks(MAX_FRAME) {
        let length = u32::try_from(chunk.len()).map_err(io::Error::other)?;
        writer.write_all(&length.to_be_bytes())?;
        writer.write_all(chunk)?;
    }
    Ok(())
}

pub fn write_end_frame(writer: &mut dyn Write) -> io::Result<()> {
    writer.write_all(&0u32.to_be_bytes())
}

pub fn copy_frames(reader: &mut dyn BufRead, writer: &mut dyn Write) -> Result<u64> {
    let mut buffer = vec![0; MAX_FRAME];
    let mut total = 0u64;
    loop {
        let mut length = [0; 4];
        reader
            .read_exact(&mut length)
            .context("read stream frame length")?;
        let length = usize::try_from(u32::from_be_bytes(length))?;
        if length == 0 {
            return Ok(total);
        }
        if length > MAX_FRAME {
            bail!("stream frame of {length} bytes exceeds the {MAX_FRAME} byte limit");
        }
        reader
            .read_exact(&mut buffer[..length])
            .context("read stream frame")?;
        writer.write_all(&buffer[..length])?;
        total += length as u64;
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{copy_frames, write_end_frame, write_frame};

    #[test]
    fn frames_round_trip_across_the_chunk_limit() {
        let payload: Vec<u8> = (0..3_000_000u32).map(|value| value as u8).collect();
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &payload).unwrap();
        write_frame(&mut encoded, b"tail").unwrap();
        write_end_frame(&mut encoded).unwrap();
        encoded.extend_from_slice(b"after the stream\n");

        let mut reader = Cursor::new(encoded);
        let mut decoded = Vec::new();
        let copied = copy_frames(&mut reader, &mut decoded).unwrap();

        assert_eq!(copied, payload.len() as u64 + 4);
        assert_eq!(&decoded[..payload.len()], &payload[..]);
        assert_eq!(&decoded[payload.len()..], b"tail");
        let mut rest = String::new();
        std::io::Read::read_to_string(&mut reader, &mut rest).unwrap();
        assert_eq!(rest, "after the stream\n");
    }
}
