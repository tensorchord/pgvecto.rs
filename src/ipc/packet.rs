use crate::bgworker::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcPacket {
    Build {
        id: Id,
        options: IndexOptions,
    },
    Insert {
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
    },
    Delete {
        id: Id,
        delete: Pointer,
    },
    Search {
        id: Id,
        target: Box<[Scalar]>,
        k: usize,
    },
    Load {
        id: Id,
    },
    Unload {
        id: Id,
    },
    Flush {
        id: Id,
    },
    Clean {
        id: Id,
    },
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BuildPacket {
    Next {},
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NextPacket {
    Leave {
        data: Option<(Box<[Scalar]>, Pointer)>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SearchPacket {
    Check { p: Pointer },
    Leave { result: Vec<Pointer> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CheckPacket {
    Leave { result: bool },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InsertPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeletePacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LoadPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum UnloadPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FlushPacket {
    Leave {},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CleanPacket {
    Leave {},
}
