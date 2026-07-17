use std::collections::HashSet;
use std::fs::{self, Metadata};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::{Deserialize, Serialize};
use sysinfo::Disks;
use walkdir::WalkDir;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Catalog {
    pub records: Vec<FileRecord>,
    pub skipped_mounts: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileRecord {
    pub absolute: PathBuf,
    pub relative: PathBuf,
    pub kind: FileKind,
    pub size: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    pub changed_seconds: i64,
    pub changed_nanoseconds: i64,
    pub device: u64,
    pub inode: u64,
    pub links: u64,
    pub mode: u32,
    pub user: u32,
    pub group: u32,
    pub link_target: Option<PathBuf>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FileKind {
    Directory,
    File,
    Symlink,
}

pub struct SourceScanner {
    source: PathBuf,
    source_is_file: bool,
    matcher: Gitignore,
    mounts: HashSet<PathBuf>,
    root_device: u64,
}

impl SourceScanner {
    pub fn new(source: &Path, exclusions: &[String]) -> Result<Self> {
        ensure_not_symlink(source)?;
        let source = fs::canonicalize(source)
            .with_context(|| format!("resolve source {}", source.display()))?;
        let metadata = fs::symlink_metadata(&source)
            .with_context(|| format!("read source metadata {}", source.display()))?;
        let source_is_file = !metadata.is_dir();
        let matcher_root = if source_is_file {
            source
                .parent()
                .context("source file has no parent directory")?
        } else {
            &source
        };
        let mut builder = GitignoreBuilder::new(matcher_root);
        for exclusion in exclusions {
            builder
                .add_line(None, exclusion)
                .with_context(|| format!("invalid exclusion pattern {exclusion:?}"))?;
        }
        let matcher = builder.build().context("build exclusion matcher")?;
        Ok(Self {
            mounts: nested_mounts(&source),
            source,
            source_is_file,
            matcher,
            root_device: metadata.dev(),
        })
    }

    pub fn scan(&self) -> Result<Catalog> {
        if self.source_is_file {
            let relative = self
                .source
                .file_name()
                .map(PathBuf::from)
                .context("source file has no name")?;
            return Ok(Catalog {
                records: vec![record(&self.source, relative)?],
                skipped_mounts: Vec::new(),
            });
        }

        let mut records = Vec::new();
        let mut skipped_mounts = Vec::new();
        let mut walker = WalkDir::new(&self.source)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter();
        while let Some(entry) = walker.next() {
            let entry = entry.with_context(|| format!("walk source {}", self.source.display()))?;
            let path = entry.path();
            if path == self.source {
                continue;
            }
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("read metadata {}", path.display()))?;
            let is_directory = metadata.is_dir();
            if self.mounts.contains(path) || metadata.dev() != self.root_device {
                if is_directory {
                    walker.skip_current_dir();
                }
                skipped_mounts.push(path.to_path_buf());
                continue;
            }
            if self
                .matcher
                .matched_path_or_any_parents(path, is_directory)
                .is_ignore()
            {
                if is_directory {
                    walker.skip_current_dir();
                }
                continue;
            }
            let relative = path
                .strip_prefix(&self.source)
                .context("walked path escaped source")?
                .to_path_buf();
            records.push(record_with_metadata(path, relative, metadata)?);
        }
        records.sort_by(|left, right| left.relative.cmp(&right.relative));
        skipped_mounts.sort();
        Ok(Catalog {
            records,
            skipped_mounts,
        })
    }
}

pub fn ensure_not_symlink(source: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("read source metadata {}", source.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("source {} must not be a symlink", source.display());
    }
    Ok(())
}

fn record(path: &Path, relative: PathBuf) -> Result<FileRecord> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read metadata {}", path.display()))?;
    record_with_metadata(path, relative, metadata)
}

fn record_with_metadata(path: &Path, relative: PathBuf, metadata: Metadata) -> Result<FileRecord> {
    let file_type = metadata.file_type();
    let kind = if file_type.is_dir() {
        FileKind::Directory
    } else if file_type.is_file() {
        FileKind::File
    } else if file_type.is_symlink() {
        FileKind::Symlink
    } else {
        bail!("unsupported special file {}", path.display());
    };
    let link_target = if kind == FileKind::Symlink {
        Some(fs::read_link(path).with_context(|| format!("read link {}", path.display()))?)
    } else {
        None
    };
    Ok(FileRecord {
        absolute: path.to_path_buf(),
        relative,
        kind,
        size: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
        device: metadata.dev(),
        inode: metadata.ino(),
        links: metadata.nlink(),
        mode: metadata.mode(),
        user: metadata.uid(),
        group: metadata.gid(),
        link_target,
        xattrs: read_xattrs(path)?,
    })
}

fn read_xattrs(path: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    let mut attributes = Vec::new();
    for name in xattr::list(path).with_context(|| format!("list xattrs {}", path.display()))? {
        let value = xattr::get(path, &name)
            .with_context(|| format!("read xattr {:?} on {}", name, path.display()))?
            .context("xattr disappeared while being read")?;
        attributes.push((name.to_string_lossy().into_owned(), value));
    }
    attributes.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(attributes)
}

fn nested_mounts(source: &Path) -> HashSet<PathBuf> {
    let mut mounts = HashSet::new();
    let disks = Disks::new_with_refreshed_list();
    for disk in disks.list() {
        let mount = disk.mount_point();
        if mount != source && mount.starts_with(source) {
            mounts.insert(mount.to_path_buf());
        }
    }
    #[cfg(target_os = "linux")]
    add_linux_mounts(source, &mut mounts);
    mounts
}

#[cfg(target_os = "linux")]
fn add_linux_mounts(source: &Path, mounts: &mut HashSet<PathBuf>) {
    let Ok(contents) = fs::read_to_string("/proc/self/mountinfo") else {
        return;
    };
    for line in contents.lines() {
        let Some(encoded) = line.split_whitespace().nth(4) else {
            continue;
        };
        let mount = PathBuf::from(decode_mount_field(encoded));
        if mount != source && mount.starts_with(source) {
            mounts.insert(mount);
        }
    }
}

#[cfg(target_os = "linux")]
fn decode_mount_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::SourceScanner;

    #[test]
    fn rejects_a_symlink_source() {
        let temporary = tempdir().unwrap();
        let real = temporary.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = temporary.path().join("link");
        symlink(&real, &link).unwrap();

        assert!(SourceScanner::new(&link, &[]).is_err());
        assert!(SourceScanner::new(&real, &[]).is_ok());
    }
}
