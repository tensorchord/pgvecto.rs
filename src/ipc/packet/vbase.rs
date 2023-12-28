use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct VbaseErrorPacket {}

#[derive(Debug, Serialize, Deserialize)]
pub enum VbasePacket {
    Next {},
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VbaseNextPacket {
    pub p: Option<Pointer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VbaseLeavePacket {}
