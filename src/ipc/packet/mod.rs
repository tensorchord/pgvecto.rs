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
        id: Id,
        options: IndexOptions,
    },
    Delete {
        id: Id,
    },
    Destory {
        ids: Vec<Id>,
    },
    Flush {
        id: Id,
    },
    Insert {
        id: Id,
        insert: (DynamicVector, Pointer),
    },
    Search {
        id: Id,
        search: (DynamicVector, usize),
        prefilter: bool,
        gucs: SearchGucs,
    },
    Stat {
        id: Id,
    },
    Vbase {
        id: Id,
        vbase: (DynamicVector, usize),
    },
}
