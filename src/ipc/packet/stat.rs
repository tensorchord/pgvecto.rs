use serde::{Deserialize, Serialize};
use service::index::IndexStat;
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum StatPacket {
    Leave {
        result: Result<IndexStat, FriendlyError>,
    },
}
