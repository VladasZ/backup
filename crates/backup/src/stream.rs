use std::io::{self, Read};

use anyhow::{Result, bail};
use blake3::Hasher;

use crate::store::Store;
use crate::store::digest::Digest;
use crate::store::recipe::Recipe;

/// Somewhere a chunk can be read from: a local store, or a remote one
/// through its agent.
pub trait ChunkSource {
    fn describe(&self) -> String;
    fn read_chunk(&mut self, id: &Digest) -> Result<Vec<u8>>;
}

impl ChunkSource for Store {
    fn describe(&self) -> String {
        self.root().display().to_string()
    }

    fn read_chunk(&mut self, id: &Digest) -> Result<Vec<u8>> {
        Store::read_chunk(self, id)
    }
}

/// A borrowed source. Sources are boxed with a `'static` object lifetime, and
/// naming it here keeps a slice of borrowed boxes usable.
pub type SourceRef<'source> = &'source mut (dyn ChunkSource + 'static);

/// Rebuilds the tar stream of a recipe. Each chunk comes from the first
/// source that has an intact copy, so one damaged chunk in one destination is
/// taken from another. The whole stream is checked against the recipe at the
/// end, and a mismatch fails the last read.
pub struct RecipeStream<'borrow, 'source> {
    sources: &'borrow mut [SourceRef<'source>],
    recipe: &'borrow Recipe,
    next: usize,
    buffer: Vec<u8>,
    offset: usize,
    hasher: Hasher,
    size: u64,
    finished: bool,
}

impl<'borrow, 'source> RecipeStream<'borrow, 'source> {
    pub fn new(sources: &'borrow mut [SourceRef<'source>], recipe: &'borrow Recipe) -> Self {
        Self {
            sources,
            recipe,
            next: 0,
            buffer: Vec::new(),
            offset: 0,
            hasher: Hasher::new(),
            size: 0,
            finished: false,
        }
    }

    fn fetch(&mut self) -> io::Result<()> {
        let chunk = self.recipe.chunks[self.next];
        let bytes = read_any(self.sources, &chunk.id).map_err(|error| {
            io::Error::other(format!("{} of {}: {error:#}", chunk.id, self.recipe.name))
        })?;
        self.hasher.update(&bytes);
        self.size += bytes.len() as u64;
        self.buffer = bytes;
        self.offset = 0;
        self.next += 1;
        Ok(())
    }

    fn check_end(&mut self) -> io::Result<()> {
        self.finished = true;
        let checksum = self.hasher.finalize().to_hex().to_string();
        if checksum != self.recipe.checksum || self.size != self.recipe.size {
            return Err(io::Error::other(format!(
                "rebuilt {} is {} bytes {checksum}, expected {} bytes {}",
                self.recipe.name, self.size, self.recipe.size, self.recipe.checksum
            )));
        }
        Ok(())
    }
}

impl Read for RecipeStream<'_, '_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.offset == self.buffer.len() {
            if self.next == self.recipe.chunks.len() {
                if !self.finished {
                    self.check_end()?;
                }
                return Ok(0);
            }
            self.fetch()?;
        }
        let available = &self.buffer[self.offset..];
        let count = available.len().min(buffer.len());
        buffer[..count].copy_from_slice(&available[..count]);
        self.offset += count;
        Ok(count)
    }
}

/// The chunk from the first source with an intact copy.
pub fn read_any(sources: &mut [SourceRef<'_>], id: &Digest) -> Result<Vec<u8>> {
    let mut failures = Vec::new();
    for source in sources.iter_mut() {
        match source.read_chunk(id) {
            Ok(bytes) if Digest::of(&bytes) == *id => return Ok(bytes),
            Ok(_) => failures.push(format!(
                "{}: chunk does not match its hash",
                source.describe()
            )),
            Err(error) => failures.push(format!("{}: {error:#}", source.describe())),
        }
    }
    bail!("chunk {id} has no intact copy: {}", failures.join("; "))
}
