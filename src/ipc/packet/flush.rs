use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum FlushPacket {
    Leave { result: Result<(), FriendlyError> },
}
