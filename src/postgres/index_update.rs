use crate::postgres::hook_transaction::{client, flush_if_commit};
use crate::prelude::*;

pub fn update_insert(id: Id, vector: Vec<Scalar>, tid: pgrx::pg_sys::ItemPointerData) {
    flush_if_commit(id);
    let p = Pointer::from_sys(tid);
    client(|mut rpc| {
        rpc.insert(id, (vector, p)).friendly().friendly();
        rpc
    })
}

pub fn update_delete(id: Id, hook: impl Fn(Pointer) -> bool) {
    flush_if_commit(id);
    client(|rpc| {
        use crate::ipc::client::DeleteHandle;
        let mut handler = rpc.delete(id).friendly();
        loop {
            let handle = handler.handle().friendly();
            match handle {
                DeleteHandle::Next { p, x } => {
                    handler = x.leave(hook(p)).friendly();
                }
                DeleteHandle::Leave { result, x } => {
                    result.friendly();
                    break x;
                }
            }
        }
    })
}
