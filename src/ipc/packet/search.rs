use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Check {
        p: Pointer,
    },
    Leave {
        result: Result<Vec<Pointer>, FriendlyError>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientCheckPacket {
    pub result: bool,
}
