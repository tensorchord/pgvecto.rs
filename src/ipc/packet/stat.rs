use serde::{Deserialize, Serialize};
use service::index::IndexStat;

#[derive(Debug, Serialize, Deserialize)]
pub enum StatPacket {
    Leave { result: IndexStat },
}
