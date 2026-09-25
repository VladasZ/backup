use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use lz4_flex::block::{compress, decompress};
use serde::{Deserialize, Serialize};

use crate::config::BackupJob;
use crate::store::digest::{DIGEST_LEN, Digest};
use crate::store::recipe::Recipe;

pub const RESPONSE_PREFIX: &str = "BACKUP/1 ";
pub const PROTOCOL_VERSION: u32 = 4;
const MAX_FRAME: usize = 1024 * 1024;

const DATA: u8 = 1;
const REFERENCE: u8 = 2;
const MISSING: u8 = 3;

/// One request line from the controller. Requests that move chunks are
/// followed by frames on stdin, and some answer with frames after their
/// response line, as noted on each.
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
    /// Followed by frames of chunk ids every destination holds. Answers with
    /// a `StreamHeader`, record frames, and a `StreamTrailer`.
    Create {
        job: String,
        source: PathBuf,
        exclude: Vec<String>,
        pre: Option<String>,
    },
    /// Answers with a `ChunkCount`, then frames of chunk ids.
    Chunks {
        destination: PathBuf,
    },
    /// Followed by record frames and one recipe line.
    Receive {
        destination: PathBuf,
        job: BackupJob,
    },
    List {
        destination: PathBuf,
        job: Option<String>,
    },
    ReadRecipe {
        destination: PathBuf,
        name: String,
    },
    /// Followed by frames of chunk ids. Answers with record frames, one per id
    /// in order, a missing record for a chunk it cannot read.
    ReadChunks {
        destination: PathBuf,
    },
    /// Followed by frames of the tar stream.
    Restore {
        target: PathBuf,
    },
    Check {
        destination: PathBuf,
    },
    /// Followed by record frames with good copies of bad chunks.
    Repair {
        destination: PathBuf,
        damaged: Vec<Digest>,
        recipes: Vec<Recipe>,
    },
    VerifyArchive {
        destination: PathBuf,
        name: String,
    },
    Prune {
        destination: PathBuf,
        job: BackupJob,
    },
    Legacy {
        destination: PathBuf,
    },
    Import {
        destination: PathBuf,
        name: String,
    },
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
pub struct ChunkCount {
    pub count: usize,
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

/// Reads the bytes of consecutive frames as one stream and reports the end
/// at the end frame, leaving whatever follows unread.
pub struct FrameReader<'reader> {
    inner: &'reader mut dyn BufRead,
    remaining: usize,
    done: bool,
}

impl<'reader> FrameReader<'reader> {
    pub fn new(inner: &'reader mut dyn BufRead) -> Self {
        Self {
            inner,
            remaining: 0,
            done: false,
        }
    }

    /// Reads to the end frame, so the caller can go on with what follows.
    pub fn finish(mut self) -> io::Result<()> {
        io::copy(&mut self, &mut io::sink())?;
        Ok(())
    }
}

impl Read for FrameReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.done || buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut length = [0; 4];
            self.inner.read_exact(&mut length)?;
            let length = u32::from_be_bytes(length) as usize;
            if length == 0 {
                self.done = true;
                return Ok(0);
            }
            if length > MAX_FRAME {
                return Err(io::Error::other(format!(
                    "stream frame of {length} bytes exceeds the {MAX_FRAME} byte limit"
                )));
            }
            self.remaining = length;
        }
        let wanted = self.remaining.min(buffer.len());
        let read = self.inner.read(&mut buffer[..wanted])?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.remaining -= read;
        Ok(read)
    }
}

pub fn write_ids<'ids>(
    writer: &mut dyn Write,
    ids: impl IntoIterator<Item = &'ids Digest>,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(MAX_FRAME);
    for id in ids {
        buffer.extend_from_slice(id.as_bytes());
        if buffer.len() + DIGEST_LEN > MAX_FRAME {
            write_frame(writer, &buffer)?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        write_frame(writer, &buffer)?;
    }
    write_end_frame(writer)
}

pub fn read_ids(reader: &mut dyn BufRead) -> Result<Vec<Digest>> {
    let mut frames = FrameReader::new(reader);
    let mut ids = Vec::new();
    let mut id = [0; DIGEST_LEN];
    loop {
        match read_full(&mut frames, &mut id)? {
            0 => return Ok(ids),
            DIGEST_LEN => ids.push(Digest::from_bytes(id)),
            partial => bail!("chunk id list ended inside an id after {partial} bytes"),
        }
    }
}

/// A chunk on the wire. Data is sent lz4 compressed and checked against its
/// id on arrival, so a transfer can never store a damaged chunk.
pub enum Record {
    Data { id: Digest, bytes: Vec<u8> },
    Reference { id: Digest, size: u32 },
    Missing { id: Digest },
}

pub fn encode_data(id: &Digest, plain: &[u8]) -> Result<Vec<u8>> {
    let packed = compress(plain);
    let compressed = packed.len() < plain.len();
    let stored = if compressed { &packed[..] } else { plain };
    let mut record = Vec::with_capacity(1 + DIGEST_LEN + 9 + stored.len());
    record.push(DATA);
    record.extend_from_slice(id.as_bytes());
    record.extend_from_slice(&u32::try_from(plain.len())?.to_le_bytes());
    record.extend_from_slice(&u32::try_from(stored.len())?.to_le_bytes());
    record.push(u8::from(compressed));
    record.extend_from_slice(stored);
    Ok(record)
}

pub fn encode_reference(id: &Digest, size: u32) -> Vec<u8> {
    let mut record = Vec::with_capacity(1 + DIGEST_LEN + 4);
    record.push(REFERENCE);
    record.extend_from_slice(id.as_bytes());
    record.extend_from_slice(&size.to_le_bytes());
    record
}

pub fn encode_missing(id: &Digest) -> Vec<u8> {
    let mut record = Vec::with_capacity(1 + DIGEST_LEN);
    record.push(MISSING);
    record.extend_from_slice(id.as_bytes());
    record
}

/// The next record, or `None` at the end of the frames.
pub fn read_record(reader: &mut dyn Read) -> Result<Option<Record>> {
    let mut tag = [0; 1];
    if read_full(reader, &mut tag)? == 0 {
        return Ok(None);
    }
    let mut id = [0; DIGEST_LEN];
    reader.read_exact(&mut id).context("read chunk id")?;
    let id = Digest::from_bytes(id);
    match tag[0] {
        DATA => {
            let mut sizes = [0; 9];
            reader.read_exact(&mut sizes).context("read chunk sizes")?;
            let plain = u32::from_le_bytes(sizes[0..4].try_into()?) as usize;
            let stored = u32::from_le_bytes(sizes[4..8].try_into()?) as usize;
            let mut bytes = vec![0; stored];
            reader.read_exact(&mut bytes).context("read chunk bytes")?;
            let bytes = if sizes[8] == 1 {
                decompress(&bytes, plain).with_context(|| format!("decompress chunk {id}"))?
            } else {
                bytes
            };
            if bytes.len() != plain || Digest::of(&bytes) != id {
                bail!("chunk {id} was damaged in transfer");
            }
            Ok(Some(Record::Data { id, bytes }))
        }
        REFERENCE => {
            let mut size = [0; 4];
            reader.read_exact(&mut size).context("read chunk size")?;
            Ok(Some(Record::Reference {
                id,
                size: u32::from_le_bytes(size),
            }))
        }
        MISSING => Ok(Some(Record::Missing { id })),
        other => bail!("unknown record type {other}"),
    }
}

// Like read_exact, but it reports how much arrived before the end instead of
// failing, so a clean end between records is not an error.
fn read_full(reader: &mut dyn Read, buffer: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use super::{
        FrameReader, Record, encode_data, encode_missing, encode_reference, read_ids, read_record,
        write_end_frame, write_frame, write_ids,
    };
    use crate::store::digest::Digest;

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
        FrameReader::new(&mut reader)
            .read_to_end(&mut decoded)
            .unwrap();

        assert_eq!(&decoded[..payload.len()], &payload[..]);
        assert_eq!(&decoded[payload.len()..], b"tail");
        let mut rest = String::new();
        reader.read_to_string(&mut rest).unwrap();
        assert_eq!(rest, "after the stream\n");
    }

    #[test]
    fn records_and_ids_round_trip() {
        let text = b"repeated text ".repeat(500);
        let id = Digest::of(&text);
        let mut encoded = Vec::new();
        for record in [
            encode_data(&id, &text).unwrap(),
            encode_reference(&id, 7),
            encode_missing(&id),
        ] {
            write_frame(&mut encoded, &record).unwrap();
        }
        write_end_frame(&mut encoded).unwrap();
        write_ids(&mut encoded, [&id, &Digest::of(b"x")]).unwrap();

        let mut reader = Cursor::new(encoded);
        {
            let mut frames = FrameReader::new(&mut reader);
            assert!(
                matches!(read_record(&mut frames).unwrap(), Some(Record::Data { bytes, .. }) if bytes == text)
            );
            assert!(matches!(
                read_record(&mut frames).unwrap(),
                Some(Record::Reference { size: 7, .. })
            ));
            assert!(matches!(
                read_record(&mut frames).unwrap(),
                Some(Record::Missing { .. })
            ));
            assert!(read_record(&mut frames).unwrap().is_none());
        }
        assert_eq!(read_ids(&mut reader).unwrap(), vec![id, Digest::of(b"x")]);
    }

    #[test]
    fn a_damaged_data_record_is_rejected() {
        let id = Digest::of(b"chunk");
        let mut record = encode_data(&id, b"chunk").unwrap();
        let last = record.len() - 1;
        record[last] ^= 0xff;
        assert!(read_record(&mut Cursor::new(record)).is_err());
    }
}
