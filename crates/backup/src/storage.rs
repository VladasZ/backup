use std::path::Path;

use anyhow::{Result, bail};
use fs2::{available_space, total_space};
use tracing::warn;

const WARNING_PERCENT: u64 = 80;
const RESERVED_PERCENT: u64 = 5;
const RESERVED_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct StorageUsage {
    pub total: u64,
    pub available: u64,
    pub occupied_percent: u64,
    pub required_reserve: u64,
}

pub fn usage(path: &Path) -> Result<StorageUsage> {
    let total = total_space(path)?;
    let available = available_space(path)?;
    let occupied_percent = available
        .saturating_mul(100)
        .checked_div(total)
        .map_or(0, |available_percent| {
            100u64.saturating_sub(available_percent)
        });
    let required_reserve = RESERVED_BYTES.max(total.saturating_mul(RESERVED_PERCENT) / 100);
    Ok(StorageUsage {
        total,
        available,
        occupied_percent,
        required_reserve,
    })
}

pub fn warn_if_high(path: &Path, purpose: &str) -> Result<StorageUsage> {
    let usage = usage(path)?;
    if usage.occupied_percent >= WARNING_PERCENT {
        warn!(
            path = %path.display(),
            occupied_percent = usage.occupied_percent,
            available = usage.available,
            total = usage.total,
            purpose,
            "filesystem usage is at or above the warning threshold"
        );
    }
    Ok(usage)
}

pub fn require_staging_reserve(path: &Path) -> Result<()> {
    let usage = warn_if_high(path, "staging")?;
    if usage.available < usage.required_reserve {
        bail!(
            "staging filesystem has {} bytes available but must reserve {} bytes",
            usage.available,
            usage.required_reserve
        );
    }
    Ok(())
}
