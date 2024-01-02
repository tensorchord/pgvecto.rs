pub mod create;
pub mod delete;
pub mod destory;
pub mod flush;
pub mod insert;
pub mod search;
pub mod stat;
pub mod vbase;

use serde::{Deserialize, Serialize};
use service::index::segments::SearchGucs;
use service::index::IndexOptions;
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
    Destory {
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
        search: (DynamicVector, usize),
        prefilter: bool,
        gucs: SearchGucs,
    },
    Stat {
        handle: Handle,
    },
    Vbase {
        handle: Handle,
        vbase: (DynamicVector, usize),
    },
}
