use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::digest::Digest;

/// One backup: the tar stream of the source, as the ordered list of chunks
/// that rebuild it, plus the blake3 and length of the whole stream.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Recipe {
    pub job: String,
    pub name: String,
    pub created: DateTime<Utc>,
    pub size: u64,
    pub checksum: String,
    pub chunks: Vec<RecipeChunk>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct RecipeChunk {
    pub id: Digest,
    pub size: u32,
}

/// What a listing shows without reading the chunks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecipeInfo {
    pub name: String,
    pub job: String,
    pub created: DateTime<Utc>,
    pub size: u64,
    pub checksum: String,
    /// The data this backup added to the store compared with the backup of
    /// its job before it, and those bytes as stored after compression. Only
    /// `Store::list_sized` fills them.
    #[serde(default)]
    pub new_size: Option<u64>,
    #[serde(default)]
    pub new_stored: Option<u64>,
}

impl Recipe {
    pub fn info(&self) -> RecipeInfo {
        RecipeInfo {
            name: self.name.clone(),
            job: self.job.clone(),
            created: self.created,
            size: self.size,
            checksum: self.checksum.clone(),
            new_size: None,
            new_stored: None,
        }
    }
}
