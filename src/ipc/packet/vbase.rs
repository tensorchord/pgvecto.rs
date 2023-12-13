use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientPacket {
    Next {},
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerNextPacket {
    pub p: Pointer,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerLeavePacket {}
