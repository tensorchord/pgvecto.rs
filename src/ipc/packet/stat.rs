use serde::{Deserialize, Serialize};
use service::index::IndexStat;
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Leave {
        result: Result<IndexStat, FriendlyError>,
    },
}
