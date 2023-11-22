use crate::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};

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
        id: Id,
    },
    Insert {
        id: Id,
        insert: (Vec<Scalar>, Pointer),
    },
    Delete {
        id: Id,
    },
    Search {
        id: Id,
        search: (Vec<Scalar>, usize),
        prefilter: bool,
    },
    StatIndexing {
        id: Id,
    },
    StatTuples {
        id: Id,
    },
    StatTuplesDone {
        id: Id,
    },
    StatConfig {
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
    Next { p: Pointer },
    Leave { result: Result<(), FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeleteNextPacket {
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
pub enum StatIndexingPacket {
    Leave { result: Result<bool, FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatTuplesPacket {
    Leave { result: Result<u32, FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatTuplesDonePacket {
    Leave { result: Result<u32, FriendlyError> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatConfigPacket {
    Leave {
        result: Result<String, FriendlyError>,
    },
}
