use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VersionError {
    #[error("Invalid version.")]
    InvalidVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub soft_version: Option<u64>,
}

impl Version {
    const VERSION: u64 = 4;
    const SOFT_VERSION: u64 = 0;
}

impl Version {
    pub fn write(path: impl AsRef<Path>) {
        let version = Version {
            version: Some(Self::VERSION),
            soft_version: Some(Self::SOFT_VERSION),
        };
        let contents = serde_json::to_string(&version).unwrap();
        std::fs::write(path, contents).unwrap();
    }
    pub fn read(path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
        use VersionError::*;
        let contents = std::fs::read_to_string(path)?;
        let version = serde_json::from_str::<Version>(&contents)?;
        if Self::VERSION != version.version.ok_or(InvalidVersion)? {
            return Err(Box::new(InvalidVersion));
        }
        if Self::SOFT_VERSION < version.soft_version.ok_or(InvalidVersion)? {
            return Err(Box::new(InvalidVersion));
        }
        Ok(())
    }
}
