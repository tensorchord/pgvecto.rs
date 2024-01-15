use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct BasicErrorPacket {}

#[derive(Debug, Serialize, Deserialize)]
pub enum BasicPacket {
    Next {},
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BasicNextPacket {
    pub p: Option<Pointer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BasicLeavePacket {}
