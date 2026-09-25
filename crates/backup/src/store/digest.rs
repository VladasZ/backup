use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use anyhow::{Context, Result};
use blake3::Hash;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const DIGEST_LEN: usize = 32;

// A blake3 hash names every chunk, pack and index file, so a plain rehash of
// the content proves it is intact. Unlike blake3::Hash it is ordered, which
// keeps listings and reports stable.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest([u8; DIGEST_LEN]);

impl Digest {
    pub fn of(bytes: &[u8]) -> Self {
        Self::from_hash(blake3::hash(bytes))
    }

    pub fn from_hash(hash: Hash) -> Self {
        Self(*hash.as_bytes())
    }

    pub fn from_bytes(bytes: [u8; DIGEST_LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        Hash::from_bytes(self.0).to_hex().to_string()
    }

    pub fn parse(hex: &str) -> Result<Self> {
        Hash::from_hex(hex)
            .map(Self::from_hash)
            .with_context(|| format!("invalid blake3 digest {hex:?}"))
    }
}

impl Display for Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(&self.to_hex())
    }
}

impl Debug for Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        Self::parse(&hex).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Digest;

    #[test]
    fn a_digest_round_trips_through_json_as_hex() {
        let digest = Digest::of(b"chunk");
        let json = serde_json::to_string(&digest).unwrap();
        assert_eq!(json, format!("\"{}\"", digest.to_hex()));
        let back: Digest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, digest);
        assert!(serde_json::from_str::<Digest>("\"not hex\"").is_err());
    }
}
