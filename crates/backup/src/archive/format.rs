use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write, empty};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

use anyhow::{Context, Result};
use lz4_flex::frame::FrameEncoder;
use tar::{Builder, EntryType, Header, HeaderMode};

use super::catalog::{Entry, FileKind};

pub struct ArchiveWriter<W: Write> {
    builder: Builder<FrameEncoder<W>>,
    hard_links: HashMap<(u64, u64), PathBuf>,
}

impl<W: Write> ArchiveWriter<W> {
    pub fn new(writer: W) -> Self {
        let mut builder = Builder::new(FrameEncoder::new(writer));
        builder.mode(HeaderMode::Complete);
        builder.follow_symlinks(false);
        Self {
            builder,
            hard_links: HashMap::new(),
        }
    }

    pub fn append(&mut self, entry: &Entry) -> Result<()> {
        append_xattrs(&mut self.builder, &entry.xattrs)?;
        let mut header = Header::new_gnu();
        header.set_metadata(&entry.metadata);
        match entry.kind {
            FileKind::Directory => {
                header.set_entry_type(EntryType::Directory);
                header.set_size(0);
                self.builder
                    .append_data(&mut header, &entry.relative, empty())?;
            }
            FileKind::Symlink => {
                header.set_entry_type(EntryType::Symlink);
                header.set_size(0);
                header.set_link_name(
                    entry
                        .link_target
                        .as_ref()
                        .context("symlink catalog entry has no target")?,
                )?;
                self.builder
                    .append_data(&mut header, &entry.relative, empty())?;
            }
            FileKind::File => {
                let key = (entry.metadata.dev(), entry.metadata.ino());
                if entry.metadata.nlink() > 1
                    && let Some(first_path) = self.hard_links.get(&key)
                {
                    header.set_entry_type(EntryType::Link);
                    header.set_size(0);
                    header.set_link_name(first_path)?;
                    self.builder
                        .append_data(&mut header, &entry.relative, empty())?;
                } else {
                    if entry.metadata.nlink() > 1 {
                        self.hard_links.insert(key, entry.relative.clone());
                    }
                    let file = File::open(&entry.absolute).with_context(|| {
                        format!("open source file {}", entry.absolute.display())
                    })?;
                    let size = entry.metadata.len();
                    header.set_entry_type(EntryType::Regular);
                    header.set_size(size);
                    self.builder.append_data(
                        &mut header,
                        &entry.relative,
                        ExactLength {
                            file,
                            remaining: size,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        self.builder.finish()?;
        let encoder = self.builder.into_inner().context("finish TAR archive")?;
        encoder.finish().context("finish LZ4 frame")
    }
}

// The tar header already promised exactly this many bytes. A file that shrank after the
// scan is padded with zeros and a file that grew is cut, so every later header stays aligned.
struct ExactLength {
    file: File,
    remaining: u64,
}

impl Read for ExactLength {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let limit = usize::try_from(self.remaining)
            .map_or(buffer.len(), |remaining| remaining.min(buffer.len()));
        let read = self.file.read(&mut buffer[..limit])?;
        let read = if read == 0 {
            buffer[..limit].fill(0);
            limit
        } else {
            read
        };
        self.remaining -= read as u64;
        Ok(read)
    }
}

fn append_xattrs<W: Write>(
    builder: &mut Builder<W>,
    attributes: &[(String, Vec<u8>)],
) -> Result<()> {
    let headers: Vec<(String, &[u8])> = attributes
        .iter()
        .map(|(name, value)| (format!("SCHILY.xattr.{name}"), value.as_slice()))
        .collect();
    builder.append_pax_extensions(headers.iter().map(|(name, value)| (name.as_str(), *value)))?;
    Ok(())
}
