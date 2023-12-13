use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Leave { result: Result<(), FriendlyError> },
}
