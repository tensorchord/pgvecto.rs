use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum SearchPacket {
    Check { p: Pointer },
    Leave { result: Vec<Pointer> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchCheckPacket {
    pub result: bool,
}
