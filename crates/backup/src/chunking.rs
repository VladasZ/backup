use std::io::{self, Write};

use blake3::Hasher;
use fastcdc::v2020::{FastCDC, Normalization};

pub const MIN_CHUNK: usize = 256 * 1024;
pub const AVERAGE_CHUNK: usize = 1024 * 1024;
pub const MAX_CHUNK: usize = 4 * 1024 * 1024;

/// Cuts the bytes written to it into content-defined chunks. A cut depends on
/// the bytes around it, not on their offset, so an insert early in the stream
/// only changes the chunks next to it and everything after it dedups again.
pub struct Chunker<'emit> {
    buffer: Vec<u8>,
    start: usize,
    emit: &'emit mut dyn FnMut(&[u8]) -> io::Result<()>,
    hasher: Hasher,
    size: u64,
}

pub struct ChunkedStream {
    pub checksum: String,
    pub size: u64,
}

impl<'emit> Chunker<'emit> {
    pub fn new(emit: &'emit mut dyn FnMut(&[u8]) -> io::Result<()>) -> Self {
        Self {
            buffer: Vec::with_capacity(2 * MAX_CHUNK),
            start: 0,
            emit,
            hasher: Hasher::new(),
            size: 0,
        }
    }

    pub fn finish(mut self) -> io::Result<ChunkedStream> {
        while self.start < self.buffer.len() {
            self.cut()?;
        }
        Ok(ChunkedStream {
            checksum: self.hasher.finalize().to_hex().to_string(),
            size: self.size,
        })
    }

    fn cut(&mut self) -> io::Result<()> {
        let pending = &self.buffer[self.start..];
        let chunker = FastCDC::with_level(
            pending,
            MIN_CHUNK,
            AVERAGE_CHUNK,
            MAX_CHUNK,
            Normalization::Level1,
        );
        let (_, end) = chunker.cut(0, pending.len());
        (self.emit)(&pending[..end])?;
        self.start += end;
        // The consumed front is dropped in one move per few chunks, not per
        // chunk, so bytes are shifted about once instead of once per cut.
        if self.start >= MAX_CHUNK {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
        Ok(())
    }
}

impl Write for Chunker<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        self.hasher.update(bytes);
        self.size += bytes.len() as u64;
        while self.buffer.len() - self.start >= MAX_CHUNK {
            self.cut()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::{Chunker, MAX_CHUNK, MIN_CHUNK};

    fn noise(length: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn chunk(data: &[u8], write_size: usize) -> (Vec<Vec<u8>>, String) {
        let mut chunks = Vec::new();
        let mut emit = |bytes: &[u8]| -> io::Result<()> {
            chunks.push(bytes.to_vec());
            Ok(())
        };
        let mut chunker = Chunker::new(&mut emit);
        for piece in data.chunks(write_size) {
            chunker.write_all(piece).unwrap();
        }
        let stream = chunker.finish().unwrap();
        (chunks, stream.checksum)
    }

    #[test]
    fn chunks_rebuild_the_stream_and_respect_the_size_limits() {
        let data = noise(20 * 1024 * 1024, 7);
        let (chunks, checksum) = chunk(&data, 64 * 1024);
        assert_eq!(chunks.concat(), data);
        assert_eq!(checksum, blake3::hash(&data).to_hex().to_string());
        for piece in &chunks[..chunks.len() - 1] {
            assert!(piece.len() >= MIN_CHUNK && piece.len() <= MAX_CHUNK);
        }
    }

    #[test]
    fn cuts_do_not_depend_on_how_the_bytes_were_written() {
        let data = noise(9 * 1024 * 1024, 3);
        assert_eq!(chunk(&data, 1000).0, chunk(&data, 5 * 1024 * 1024).0);
    }

    #[test]
    fn an_insert_near_the_start_leaves_later_chunks_unchanged() {
        let data = noise(16 * 1024 * 1024, 11);
        let mut edited = data[..100].to_vec();
        edited.extend_from_slice(b"inserted bytes");
        edited.extend_from_slice(&data[100..]);
        let (before, _) = chunk(&data, 1 << 16);
        let (after, _) = chunk(&edited, 1 << 16);
        let shared = after.iter().filter(|piece| before.contains(piece)).count();
        assert!(
            shared + 2 >= before.len(),
            "only {shared} of {} chunks survived an insert",
            before.len()
        );
    }
}
