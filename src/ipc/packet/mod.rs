pub mod create;
pub mod delete;
pub mod destroy;
pub mod flush;
pub mod insert;
pub mod search;
pub mod stat;
pub mod vbase;

use serde::{Deserialize, Serialize};
use service::index::IndexOptions;
use service::index::SearchOptions;
use service::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcPacket {
    Create {
        handle: Handle,
        options: IndexOptions,
    },
    Delete {
        handle: Handle,
    },
    Destroy {
        handle: Handle,
    },
    Flush {
        handle: Handle,
    },
    Insert {
        handle: Handle,
        insert: (DynamicVector, Pointer),
    },
    Search {
        handle: Handle,
        vector: DynamicVector,
        prefilter: bool,
        opts: SearchOptions,
    },
    Stat {
        handle: Handle,
    },
    Vbase {
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
    },
}
