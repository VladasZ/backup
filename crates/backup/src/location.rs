use std::fmt::{Display, Formatter, Result as FmtResult};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Error, Result, bail};
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

const SSH_PATH_ENCODE: &AsciiSet = &CONTROLS.add(b' ').add(b'"').add(b'#').add(b'%').add(b'?');

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Location {
    Local(PathBuf),
    Ssh(SshLocation),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SshLocation {
    pub host: String,
    pub user: Option<String>,
    pub port: Option<u16>,
    pub path: PathBuf,
}

impl Location {
    pub fn path(&self) -> &Path {
        match self {
            Self::Local(path) => path,
            Self::Ssh(remote) => &remote.path,
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Local(parent), Self::Local(child)) => child.starts_with(parent),
            (Self::Ssh(parent), Self::Ssh(child)) => {
                parent.same_host(child) && child.path.starts_with(&parent.path)
            }
            _ => false,
        }
    }
}

impl SshLocation {
    pub fn target(&self) -> String {
        match &self.user {
            Some(user) => format!("{user}@{}", self.host),
            None => self.host.clone(),
        }
    }

    pub fn same_host(&self, other: &Self) -> bool {
        self.host == other.host && self.user == other.user && self.port == other.port
    }
}

impl FromStr for Location {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.starts_with("ssh://") {
            return parse_ssh(value).map(Self::Ssh);
        }
        if value.contains("://") {
            bail!("unsupported location scheme in {value:?}");
        }
        let path = PathBuf::from(value);
        validate_path(&path, "local path")?;
        Ok(Self::Local(path))
    }
}

impl Display for Location {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Local(path) => write!(formatter, "{}", path.display()),
            Self::Ssh(remote) => {
                write!(formatter, "ssh://")?;
                if let Some(user) = &remote.user {
                    write!(formatter, "{user}@")?;
                }
                write!(formatter, "{}", remote.host)?;
                if let Some(port) = remote.port {
                    write!(formatter, ":{port}")?;
                }
                write!(
                    formatter,
                    "{}",
                    utf8_percent_encode(&remote.path.to_string_lossy(), SSH_PATH_ENCODE)
                )
            }
        }
    }
}

impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Location {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

fn parse_ssh(value: &str) -> Result<SshLocation> {
    let url = Url::parse(value).with_context(|| format!("invalid SSH location {value:?}"))?;
    if url.scheme() != "ssh" {
        bail!("only ssh:// remote locations are supported");
    }
    if url.password().is_some() {
        bail!("SSH passwords are not allowed in the configuration");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("SSH locations cannot contain a query or fragment");
    }
    let host = url.host_str().context("SSH location is missing a host")?;
    let decoded = percent_decode_str(url.path())
        .decode_utf8()
        .context("SSH path is not valid UTF-8")?;
    let path = PathBuf::from(decoded.as_ref());
    validate_path(&path, "SSH path")?;
    let user = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_owned())
    };
    Ok(SshLocation {
        host: host.to_owned(),
        user,
        port: url.port(),
        path,
    })
}

fn validate_path(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} must be absolute: {}", path.display());
    }
    if path.components().any(|part| part == Component::ParentDir) {
        bail!("{label} cannot contain '..': {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Location, SshLocation};
    use std::path::PathBuf;

    #[test]
    fn parses_ssh_alias() {
        let parsed: Location = "ssh://user@pc1:2222/mnt/backup".parse().unwrap();
        assert_eq!(
            parsed,
            Location::Ssh(SshLocation {
                host: "pc1".to_owned(),
                user: Some("user".to_owned()),
                port: Some(2222),
                path: PathBuf::from("/mnt/backup"),
            })
        );
    }

    #[test]
    fn rejects_relative_paths() {
        assert!("relative/path".parse::<Location>().is_err());
    }

    #[test]
    fn ssh_location_round_trips_reserved_path_characters() {
        let location: Location = "ssh://pc1/a%20path/file%23one".parse().unwrap();
        assert_eq!(location.to_string(), "ssh://pc1/a%20path/file%23one");
        assert_eq!(location.to_string().parse::<Location>().unwrap(), location);
    }
}
