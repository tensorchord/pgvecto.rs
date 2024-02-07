pub mod basic;
pub mod create;
pub mod delete;
pub mod drop;
pub mod flush;
pub mod insert;
pub mod list;
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
    Flush {
        handle: Handle,
    },
    Drop {
        handle: Handle,
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
        pointer: Pointer,
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
    List {
        handle: Handle,
    },
    // admin
    Upgrade {},
}
