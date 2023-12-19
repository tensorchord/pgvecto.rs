use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use thiserror::Error;

#[repr(u64)]
enum Version {
    V1 = 1,
}

const VERSION: Version = Version::V1;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Invalid version.")]
    InvalidVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub version: Option<u64>,
}

impl Metadata {
    pub fn write(path: impl AsRef<Path>) {
        let metadata = Metadata {
            version: Some(VERSION as u64),
        };
        let contents = serde_json::to_string(&metadata).unwrap();
        std::fs::write(path, contents).unwrap();
    }
    pub fn read(path: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
        let contents = std::fs::read_to_string(path)?;
        let metadata = serde_json::from_str::<Metadata>(&contents)?;
        if metadata.version != Some(VERSION as u64) {
            return Err(Box::new(MetadataError::InvalidVersion));
        }
        Ok(())
    }
}
