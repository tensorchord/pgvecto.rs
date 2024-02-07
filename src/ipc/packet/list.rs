use serde::{Deserialize, Serialize};
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct ListErrorPacket {}

#[derive(Debug, Serialize, Deserialize)]
pub enum ListPacket {
    Next {},
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListNextPacket {
    pub p: Option<Pointer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListLeavePacket {}
