use crate::postgres::hook_transaction::{client, flush_if_commit};
use crate::prelude::*;

pub unsafe fn update_insert(id: Id, vector: Vec<Scalar>, tid: pgrx::pg_sys::ItemPointer) {
    flush_if_commit(id);
    let p = Pointer::from_sys(*tid);
    client(|mut rpc| {
        rpc.insert(id, (vector, p)).unwrap().friendly();
        rpc
    })
}

pub fn update_delete(id: Id, hook: impl Fn(Pointer) -> bool) {
    flush_if_commit(id);
    client(|rpc| {
        use crate::ipc::client::DeleteHandle;
        let mut handler = rpc.delete(id).unwrap();
        loop {
            let handle = handler.handle().unwrap();
            match handle {
                DeleteHandle::Next { p, x } => {
                    handler = x.leave(hook(p)).unwrap();
                }
                DeleteHandle::Leave { result, x } => {
                    result.friendly();
                    break x;
                }
            }
        }
    })
}
