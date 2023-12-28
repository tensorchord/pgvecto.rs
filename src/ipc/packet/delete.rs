use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum DeletePacket {
    Test { p: Pointer },
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteTestPacket {
    pub delete: bool,
}
