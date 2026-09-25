//! A pack holds many chunks. Its payload is the stored chunks back to back,
//! then a JSON directory of them, then the directory length as 8 bytes. The
//! payload is sealed with parity, and the pack is named by the blake3 of its
//! payload, so the index can always be rebuilt from the packs alone.
//!
//! The directory carries a random salt, so two packs never share a name even
//! when they hold the same chunks. Without it, a repair that rewrites the
//! chunks of a damaged pack would produce the damaged pack's own name.

use anyhow::{Context, Result, bail};
use lz4_flex::block::{compress, decompress};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::digest::Digest;
use super::seal::seal;

pub const PACK_TARGET: usize = 32 * 1024 * 1024;
const LENGTH_LEN: usize = 8;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PackEntry {
    pub id: Digest,
    pub offset: u64,
    pub stored: u32,
    pub size: u32,
    pub compressed: bool,
}

#[derive(Deserialize, Serialize)]
struct Directory {
    salt: Uuid,
    entries: Vec<PackEntry>,
}

pub struct SealedPack {
    pub id: Digest,
    pub bytes: Vec<u8>,
    pub entries: Vec<PackEntry>,
}

#[derive(Default)]
pub struct PackBuilder {
    payload: Vec<u8>,
    entries: Vec<PackEntry>,
}

impl PackBuilder {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.payload.len() >= PACK_TARGET
    }

    pub fn add(&mut self, id: Digest, plain: &[u8]) -> Result<()> {
        let packed = compress(plain);
        let compressed = packed.len() < plain.len();
        let stored = if compressed { &packed[..] } else { plain };
        self.entries.push(PackEntry {
            id,
            offset: u64::try_from(self.payload.len())?,
            stored: u32::try_from(stored.len())?,
            size: u32::try_from(plain.len())?,
            compressed,
        });
        self.payload.extend_from_slice(stored);
        Ok(())
    }

    pub fn seal(mut self) -> Result<SealedPack> {
        let directory = Directory {
            salt: Uuid::new_v4(),
            entries: self.entries,
        };
        let encoded = serde_json::to_vec(&directory).context("encode pack directory")?;
        self.payload.extend_from_slice(&encoded);
        self.payload
            .extend_from_slice(&u64::try_from(encoded.len())?.to_le_bytes());
        Ok(SealedPack {
            id: Digest::of(&self.payload),
            bytes: seal(&self.payload)?,
            entries: directory.entries,
        })
    }
}

pub fn read_directory(payload: &[u8]) -> Result<Vec<PackEntry>> {
    let split = payload
        .len()
        .checked_sub(LENGTH_LEN)
        .context("pack is too short to hold a directory")?;
    let length = u64::from_le_bytes(payload[split..].try_into()?);
    let start = split
        .checked_sub(usize::try_from(length)?)
        .context("pack directory length is larger than the pack")?;
    let directory: Directory =
        serde_json::from_slice(&payload[start..split]).context("decode pack directory")?;
    Ok(directory.entries)
}

/// The chunk bytes, checked against the id they are stored under.
pub fn extract(payload: &[u8], entry: &PackEntry) -> Result<Vec<u8>> {
    let start = usize::try_from(entry.offset)?;
    let end = start + usize::try_from(entry.stored)?;
    let stored = payload
        .get(start..end)
        .with_context(|| format!("chunk {} lies outside its pack", entry.id))?;
    let plain = if entry.compressed {
        decompress(stored, usize::try_from(entry.size)?)
            .with_context(|| format!("decompress chunk {}", entry.id))?
    } else {
        stored.to_vec()
    };
    if plain.len() != usize::try_from(entry.size)? || Digest::of(&plain) != entry.id {
        bail!("chunk {} does not match its hash", entry.id);
    }
    Ok(plain)
}

#[cfg(test)]
mod tests {
    use super::{PackBuilder, extract, read_directory};
    use crate::store::digest::Digest;
    use crate::store::seal::unseal;

    #[test]
    fn chunks_come_back_out_of_a_sealed_pack() {
        let text = b"compressible ".repeat(1000);
        let noise: Vec<u8> = (0..4096u32)
            .map(|value| (value.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let mut builder = PackBuilder::default();
        builder.add(Digest::of(&text), &text).unwrap();
        builder.add(Digest::of(&noise), &noise).unwrap();
        let pack = builder.seal().unwrap();

        let payload = unseal(&pack.bytes).unwrap().payload;
        assert_eq!(Digest::of(&payload), pack.id);
        let entries = read_directory(&payload).unwrap();
        assert!(entries[0].compressed);
        assert_eq!(extract(&payload, &entries[0]).unwrap(), text);
        assert_eq!(extract(&payload, &entries[1]).unwrap(), noise);
    }

    #[test]
    fn packs_with_the_same_chunks_get_different_names() {
        let seal_one = || {
            let mut builder = PackBuilder::default();
            builder.add(Digest::of(b"same"), b"same").unwrap();
            builder.seal().unwrap().id
        };
        assert_ne!(seal_one(), seal_one());
    }

    #[test]
    fn a_chunk_under_the_wrong_id_is_rejected() {
        let mut builder = PackBuilder::default();
        builder.add(Digest::of(b"other"), b"chunk").unwrap();
        let pack = builder.seal().unwrap();
        let payload = unseal(&pack.bytes).unwrap().payload;
        let entries = read_directory(&payload).unwrap();
        assert!(extract(&payload, &entries[0]).is_err());
    }
}
