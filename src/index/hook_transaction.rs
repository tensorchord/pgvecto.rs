use crate::utils::cells::PgRefCell;
use service::prelude::*;
use std::collections::BTreeSet;

static FLUSH_IF_COMMIT: PgRefCell<BTreeSet<Handle>> = unsafe { PgRefCell::new(BTreeSet::new()) };

pub fn aborting() {
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn committing() {
    {
        let flush_if_commit = FLUSH_IF_COMMIT.borrow();
        if flush_if_commit.len() != 0 {
            let mut rpc = crate::ipc::client::borrow_mut();
            for id in flush_if_commit.iter().copied() {
                rpc.flush(id);
            }
        }
    }
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn flush_if_commit(handle: Handle) {
    FLUSH_IF_COMMIT.borrow_mut().insert(handle);
}
