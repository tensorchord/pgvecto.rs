pub mod abort;
pub mod basic;
pub mod commit;
pub mod create;
pub mod delete;
pub mod insert;
pub mod stat;
pub mod upgrade;
pub mod vbase;

use serde::{Deserialize, Serialize};
use service::index::IndexOptions;
use service::index::SearchOptions;
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcPacket {
    // transaction
    Commit {
        pending_deletes: Vec<Handle>,
        pending_dirty: Vec<Handle>,
    },
    Abort {
        pending_deletes: Vec<Handle>,
    },
    Create {
        handle: Handle,
        options: IndexOptions,
    },
    // instance
    Insert {
        handle: Handle,
        vector: DynamicVector,
        pointer: Pointer,
    },
    Delete {
        handle: Handle,
    },
    Stat {
        handle: Handle,
    },
    Basic {
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
    },
    Vbase {
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
    },
    // admin
    Upgrade {},
}
