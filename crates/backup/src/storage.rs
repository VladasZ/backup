use std::path::Path;

use anyhow::Result;
use fs2::{available_space, total_space};
use tracing::warn;

const WARNING_PERCENT: u64 = 80;

#[derive(Clone, Copy, Debug)]
pub struct StorageUsage {
    pub total: u64,
    pub available: u64,
    pub occupied_percent: u64,
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
    Ok(StorageUsage {
        total,
        available,
        occupied_percent,
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
