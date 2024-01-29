use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum UpgradePacket {
    Leave {},
}
