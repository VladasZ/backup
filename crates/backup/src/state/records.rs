use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::archive::Artifact;
use crate::config::BackupJob;
use crate::location::Location;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunRecord {
    pub job: BackupJob,
    pub archive_name: String,
    pub archive_path: PathBuf,
    pub checksum_path: PathBuf,
    pub checksum: String,
    pub size: u64,
    pub created_at: i64,
    pub staged: bool,
    pub completed_at: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeliveryRecord {
    pub delivered: bool,
    pub attempts: u32,
    pub next_retry: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RetryRecord {
    pub slot: i64,
    pub attempts: u32,
    pub next_retry: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryStatus {
    Delivered,
    Failed(String),
    Pending,
}

#[derive(Clone, Debug)]
pub struct DeliveryResult {
    pub destination: Location,
    pub status: DeliveryStatus,
}

#[derive(Clone, Debug)]
pub struct PendingDelivery {
    pub run_id: Uuid,
    pub artifact: Artifact,
    pub job: BackupJob,
    pub destination: Location,
    pub attempts: u32,
}

#[derive(Clone, Debug)]
pub struct CompletedRun {
    pub run_id: Uuid,
    pub archive: PathBuf,
    pub checksum: PathBuf,
    pub staged: bool,
}

#[derive(Clone, Debug)]
pub struct StatusLine {
    pub job: String,
    pub archive: String,
    pub created_at: DateTime<Utc>,
    pub pending_destinations: usize,
}

#[derive(Clone, Debug)]
pub struct HistoryLine {
    pub job: String,
    pub archive: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
pub struct BackupRetry {
    pub slot: DateTime<Utc>,
    pub attempts: u32,
    pub next_retry: DateTime<Utc>,
}
