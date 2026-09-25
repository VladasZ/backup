use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};

pub fn read_checksum(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut line = String::new();
    BufReader::new(file).read_line(&mut line)?;
    line.split_whitespace()
        .next()
        .map(str::to_owned)
        .context("checksum file is empty")
}
