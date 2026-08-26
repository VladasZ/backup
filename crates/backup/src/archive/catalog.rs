use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::{self, Metadata};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use sysinfo::Disks;
use walkdir::WalkDir;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Directory,
    File,
    Symlink,
}

pub struct Entry {
    pub absolute: PathBuf,
    pub relative: PathBuf,
    pub kind: FileKind,
    pub metadata: Metadata,
    pub link_target: Option<PathBuf>,
    pub xattrs: Vec<(String, Vec<u8>)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fingerprint {
    pub relative: PathBuf,
    pub hash: [u8; 32],
}

#[derive(Debug, Default)]
pub struct Walk {
    pub fingerprints: Vec<Fingerprint>,
    pub skipped_mounts: Vec<PathBuf>,
    pub skipped_special: Vec<PathBuf>,
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

    pub fn walk(&self, visit: &mut dyn FnMut(&Entry) -> Result<()>) -> Result<Walk> {
        let mut walk = Walk::default();
        if self.source_is_file {
            let relative = self
                .source
                .file_name()
                .map(PathBuf::from)
                .context("source file has no name")?;
            let metadata = fs::symlink_metadata(&self.source)
                .with_context(|| format!("read metadata {}", self.source.display()))?;
            let entry = entry(&self.source, relative, metadata)?
                .with_context(|| format!("source {} is a special file", self.source.display()))?;
            walk.fingerprints.push(fingerprint(&entry));
            visit(&entry)?;
            return Ok(walk);
        }

        let mut walker = WalkDir::new(&self.source)
            .follow_links(false)
            .sort_by_file_name()
            .into_iter();
        while let Some(item) = walker.next() {
            let item = item.with_context(|| format!("walk source {}", self.source.display()))?;
            let path = item.path();
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
                walk.skipped_mounts.push(path.to_path_buf());
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
            let Some(entry) = entry(path, relative, metadata)? else {
                walk.skipped_special.push(path.to_path_buf());
                continue;
            };
            walk.fingerprints.push(fingerprint(&entry));
            visit(&entry)?;
        }
        walk.fingerprints
            .sort_by(|left, right| left.relative.cmp(&right.relative));
        walk.skipped_mounts.sort();
        walk.skipped_special.sort();
        Ok(walk)
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

pub fn changed_paths(before: &[Fingerprint], after: &[Fingerprint]) -> Vec<PathBuf> {
    let mut changed = Vec::new();
    let mut before = before.iter().peekable();
    let mut after = after.iter().peekable();
    loop {
        match (before.peek(), after.peek()) {
            (None, None) => return changed,
            (Some(old), None) => {
                changed.push(old.relative.clone());
                before.next();
            }
            (None, Some(new)) => {
                changed.push(new.relative.clone());
                after.next();
            }
            (Some(old), Some(new)) => match old.relative.cmp(&new.relative) {
                Ordering::Less => {
                    changed.push(old.relative.clone());
                    before.next();
                }
                Ordering::Greater => {
                    changed.push(new.relative.clone());
                    after.next();
                }
                Ordering::Equal => {
                    if old.hash != new.hash {
                        changed.push(old.relative.clone());
                    }
                    before.next();
                    after.next();
                }
            },
        }
    }
}

fn entry(path: &Path, relative: PathBuf, metadata: Metadata) -> Result<Option<Entry>> {
    let file_type = metadata.file_type();
    let kind = if file_type.is_dir() {
        FileKind::Directory
    } else if file_type.is_file() {
        FileKind::File
    } else if file_type.is_symlink() {
        FileKind::Symlink
    } else {
        return Ok(None);
    };
    let link_target = if kind == FileKind::Symlink {
        Some(fs::read_link(path).with_context(|| format!("read link {}", path.display()))?)
    } else {
        None
    };
    let xattrs = read_xattrs(path)?;
    Ok(Some(Entry {
        absolute: path.to_path_buf(),
        relative,
        kind,
        metadata,
        link_target,
        xattrs,
    }))
}

fn fingerprint(entry: &Entry) -> Fingerprint {
    let metadata = &entry.metadata;
    let mut hasher = Hasher::new();
    hasher.update(&[entry.kind as u8]);
    for value in [
        metadata.len(),
        metadata.dev(),
        metadata.ino(),
        metadata.nlink(),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    for value in [
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    for value in [metadata.mode(), metadata.uid(), metadata.gid()] {
        hasher.update(&value.to_le_bytes());
    }
    if let Some(target) = &entry.link_target {
        hasher.update(target.as_os_str().as_encoded_bytes());
    }
    for (name, value) in &entry.xattrs {
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value);
    }
    Fingerprint {
        relative: entry.relative.clone(),
        hash: *hasher.finalize().as_bytes(),
    }
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
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{Fingerprint, SourceScanner, changed_paths};

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

    #[test]
    fn special_files_are_skipped_not_fatal() {
        let temporary = tempdir().unwrap();
        let source = temporary.path().join("s");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("keep.txt"), "keep").unwrap();
        let socket = UnixListener::bind(source.join("agent.sock")).unwrap();

        let scanner = SourceScanner::new(&source, &[]).unwrap();
        let mut seen = Vec::new();
        let walk = scanner
            .walk(&mut |entry| {
                seen.push(entry.relative.clone());
                Ok(())
            })
            .unwrap();
        drop(socket);

        assert_eq!(seen, vec![PathBuf::from("keep.txt")]);
        assert_eq!(
            walk.skipped_special,
            vec![fs::canonicalize(&source).unwrap().join("agent.sock")]
        );
        assert_eq!(walk.fingerprints.len(), 1);
    }

    #[test]
    fn changed_paths_reports_added_removed_and_modified_entries() {
        let print = |name: &str, hash: u8| Fingerprint {
            relative: PathBuf::from(name),
            hash: [hash; 32],
        };
        let before = [print("a", 1), print("b", 1), print("d", 1)];
        let after = [print("a", 1), print("b", 2), print("c", 1)];

        assert_eq!(
            changed_paths(&before, &after),
            vec![PathBuf::from("b"), PathBuf::from("c"), PathBuf::from("d")]
        );
        assert!(changed_paths(&before, &before).is_empty());
    }
}
