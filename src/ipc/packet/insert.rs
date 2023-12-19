use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum InsertPacket {
    Leave {},
}
