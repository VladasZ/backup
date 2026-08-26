use std::path::Path;

use anyhow::{Result, bail};

pub fn install(config: &Path) -> Result<()> {
    bail!(
        "cannot install a service for {}, only macOS and Linux are supported",
        config.display()
    )
}

pub fn uninstall() -> Result<()> {
    bail!("service installation is supported only on macOS and Linux")
}
