use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use super::checksum::write_checksum;
use super::create_private;
use super::stream::{Sink, SinkId};

pub struct StagingSink {
    partial: PathBuf,
    final_path: PathBuf,
    file: File,
}

impl StagingSink {
    pub fn open(staging: &Path, name: &str) -> Result<Box<dyn Sink>> {
        fs::create_dir_all(staging)
            .with_context(|| format!("create staging directory {}", staging.display()))?;
        let partial = staging.join(format!(".{name}.partial"));
        remove_if_present(&partial)?;
        let file = create_private(&partial)?;
        Ok(Box::new(Self {
            partial,
            final_path: staging.join(name),
            file,
        }))
    }
}

impl Sink for StagingSink {
    fn id(&self) -> SinkId {
        SinkId::Staging
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn finish(self: Box<Self>, checksum: &str, size: u64) -> Result<()> {
        let result = (|| {
            self.file.sync_all()?;
            let written = self.file.metadata()?.len();
            if written != size {
                bail!("staged {written} bytes, expected {size}");
            }
            fs::rename(&self.partial, &self.final_path).with_context(|| {
                format!(
                    "publish staged archive {} as {}",
                    self.partial.display(),
                    self.final_path.display()
                )
            })?;
            write_checksum(&self.final_path, checksum)?;
            let directory = self
                .final_path
                .parent()
                .context("staged archive has no parent directory")?;
            File::open(directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            remove_if_present(&self.partial)?;
        }
        result
    }

    fn abort(self: Box<Self>) -> Option<String> {
        drop(self.file);
        if let Err(error) = remove_if_present(&self.partial) {
            tracing::warn!(%error, "could not remove partial staged archive");
        }
        None
    }
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
