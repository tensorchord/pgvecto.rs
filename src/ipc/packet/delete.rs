use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Test { p: Pointer },
    Leave { result: Result<(), FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientTestPacket {
    pub delete: bool,
}
