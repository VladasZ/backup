use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::archive::Artifact;
use crate::config::{BackupJob, CompressionConfig};

pub const RESPONSE_PREFIX: &str = "BACKUP/1 ";
pub const PROTOCOL_VERSION: u32 = 1;

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
        compression: CompressionConfig,
    },
    Receive {
        artifact: WireArtifact,
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
    pub data: Option<Value>,
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
