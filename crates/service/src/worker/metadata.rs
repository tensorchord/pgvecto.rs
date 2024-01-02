use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Invalid version.")]
    InvalidVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub soft_version: Option<u64>,
}

impl Metadata {
    const VERSION: u64 = 2;
    const SOFT_VERSION: u64 = 1;
}

impl Metadata {
    pub fn write(path: impl AsRef<Path>) {
        let metadata = Metadata {
            version: Some(Self::VERSION),
            soft_version: Some(Self::SOFT_VERSION),
        };
        let contents = serde_json::to_string(&metadata).unwrap();
        std::fs::write(path, contents).unwrap();
    }
    pub fn read(path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
        use MetadataError::*;
        let contents = std::fs::read_to_string(path)?;
        let metadata = serde_json::from_str::<Metadata>(&contents)?;
        if Self::VERSION != metadata.version.ok_or(InvalidVersion)? {
            return Err(Box::new(InvalidVersion));
        }
        if Self::SOFT_VERSION < metadata.soft_version.ok_or(InvalidVersion)? {
            return Err(Box::new(InvalidVersion));
        }
        Ok(())
    }
}
