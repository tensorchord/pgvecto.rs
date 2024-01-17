use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum AbortPacket {
    Leave {},
}
