use serde::{Deserialize, Serialize};
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcPacket {
    Create {
        id: Id,
        options: IndexOptions,
    },
    Flush {
        id: Id,
    },
    Destory {
        ids: Vec<Id>,
    },
    Insert {
        id: Id,
        insert: (DynamicVector, Pointer),
    },
    Delete {
        id: Id,
    },
    Search {
        id: Id,
        search: (DynamicVector, usize),
        prefilter: bool,
    },
    Stat {
        id: Id,
    },
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CreatePacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FlushPacket {
    Leave { result: Result<(), FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DestoryPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InsertPacket {
    Leave { result: Result<(), FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeletePacket {
    Test { p: Pointer },
    Leave { result: Result<(), FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeleteTestPacket {
    Leave { delete: bool },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SearchPacket {
    Check {
        p: Pointer,
    },
    Leave {
        result: Result<Vec<Pointer>, FriendlyError>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SearchCheckPacket {
    Leave { result: bool },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatPacket {
    Leave {
        result: Result<IndexStat, FriendlyError>,
    },
}
