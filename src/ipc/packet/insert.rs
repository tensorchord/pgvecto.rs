use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum InsertPacket {
    Leave { result: Result<(), FriendlyError> },
}
