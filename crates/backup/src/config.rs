use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use croner::Cron;
use serde::{Deserialize, Serialize};

use crate::location::Location;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub compression: CompressionConfig,

    #[serde(rename = "backup")]
    pub jobs: Vec<BackupJob>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub level: Option<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Zstd,
    Zpaq,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupJob {
    pub name: String,
    pub source: Location,
    pub destinations: Vec<Location>,
    pub cron: String,
    pub retention: Option<RetentionConfig>,

    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    pub count: Option<usize>,
    pub age: Option<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read configuration {}", path.display()))?;
        let config: Self = toml::from_str(&contents)
            .with_context(|| format!("parse configuration {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.compression.validate()?;
        if self.jobs.is_empty() {
            bail!("configuration must contain at least one [[backup]] job");
        }
        let mut names = HashSet::new();
        for job in &self.jobs {
            job.validate()?;
            if !names.insert(&job.name) {
                bail!("duplicate backup job name {:?}", job.name);
            }
        }
        Ok(())
    }

    pub fn job(&self, name: &str) -> Result<&BackupJob> {
        self.jobs
            .iter()
            .find(|job| job.name == name)
            .with_context(|| format!("unknown backup job {name:?}"))
    }
}

impl CompressionConfig {
    pub fn validate(&self) -> Result<()> {
        match (self.algorithm, self.level) {
            (CompressionAlgorithm::Zpaq, Some(3..=5)) => Ok(()),
            (CompressionAlgorithm::Zpaq, Some(level)) => {
                bail!("ZPAQ level must be 3, 4, or 5, got {level}")
            }
            (CompressionAlgorithm::Zpaq, None) => bail!("ZPAQ compression requires a level"),
            (_, Some(_)) => bail!("compression level is only supported for ZPAQ"),
            (_, None) => Ok(()),
        }
    }

    pub fn extension(&self) -> &'static str {
        match self.algorithm {
            CompressionAlgorithm::None => "tar",
            CompressionAlgorithm::Gzip => "tar.gz",
            CompressionAlgorithm::Zstd => "tar.zst",
            CompressionAlgorithm::Zpaq => "tar.zpaq",
        }
    }
}

impl BackupJob {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        {
            bail!(
                "job name {:?} may contain only letters, numbers, '.', '_', and '-'",
                self.name
            );
        }
        if self.destinations.is_empty() {
            bail!("job {:?} must have at least one destination", self.name);
        }
        if self.cron.split_whitespace().count() != 5 {
            bail!("job {:?} cron must contain exactly five fields", self.name);
        }
        self.cron
            .parse::<Cron>()
            .with_context(|| format!("job {:?} has an invalid cron expression", self.name))?;
        let mut destinations = HashSet::new();
        for destination in &self.destinations {
            if self.source == *destination || self.source.contains(destination) {
                bail!(
                    "job {:?} destination {destination} is inside its source",
                    self.name
                );
            }
            if !destinations.insert(destination) {
                bail!("job {:?} repeats destination {destination}", self.name);
            }
        }
        if let Some(retention) = &self.retention {
            retention
                .validate()
                .with_context(|| format!("job {:?} has invalid retention settings", self.name))?;
        }
        for pattern in &self.exclude {
            if pattern.trim().is_empty() {
                bail!("job {:?} contains an empty exclusion pattern", self.name);
            }
        }
        Ok(())
    }

    pub fn schedule(&self) -> Result<Cron> {
        self.cron.parse().context("validated cron became invalid")
    }
}

impl RetentionConfig {
    pub fn validate(&self) -> Result<()> {
        match (&self.count, &self.age) {
            (None, None) => bail!("retention must contain either count or age"),
            (Some(_), Some(_)) => bail!("retention count and age are mutually exclusive"),
            (Some(0), None) => bail!("retention count must be greater than zero"),
            (Some(_), None) => Ok(()),
            (None, Some(age)) => {
                let duration = humantime::parse_duration(age)
                    .with_context(|| format!("invalid retention age {age:?}"))?;
                if duration.is_zero() {
                    bail!("retention age must be greater than zero");
                }
                Ok(())
            }
        }
    }

    pub fn age_duration(&self) -> Result<Option<Duration>> {
        self.age
            .as_ref()
            .map(|age| {
                humantime::parse_duration(age).context("validated retention age became invalid")
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::{CompressionAlgorithm, Config};

    #[test]
    fn parses_multiple_jobs() {
        let config: Config = toml::from_str(
            r#"
[compression]
algorithm = "zpaq"
level = 4

[[backup]]
name = "documents"
source = "/source"
destinations = ["/destination"]
cron = "0 2 * * *"
retention = { count = 30 }
exclude = ["*.tmp"]

[[backup]]
name = "photos"
source = "ssh://pc1/mnt/photos"
destinations = ["/archive"]
cron = "0 4 * * 0"
"#,
        )
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.jobs.len(), 2);
        assert_eq!(config.compression.algorithm, CompressionAlgorithm::Zpaq);
    }

    #[test]
    fn rejects_both_retention_modes() {
        let config: Config = toml::from_str(
            r#"
[compression]
algorithm = "zstd"

[[backup]]
name = "bad"
source = "/source"
destinations = ["/destination"]
cron = "0 2 * * *"
retention = { count = 3, age = "2d" }
"#,
        )
        .unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn example_configuration_stays_valid() {
        let config: Config = toml::from_str(include_str!("../../../config.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.jobs.len(), 3);
    }
}
