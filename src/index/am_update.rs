use crate::index::hook_transaction::callback_dirty;
use crate::prelude::*;
use service::prelude::*;

pub fn update_insert(handle: Handle, vector: DynamicVector, tid: pgrx::pg_sys::ItemPointerData) {
    callback_dirty(handle);

    let pointer = Pointer::from_sys(tid);
    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.insert(handle, vector, pointer);
}

pub fn update_delete(handle: Handle, f: impl Fn(Pointer) -> bool) {
    callback_dirty(handle);

    let mut rpc_list = crate::ipc::client::borrow_mut().list(handle);
    let mut rpc = crate::ipc::client::borrow_mut();
    while let Some(p) = rpc_list.next() {
        if f(p) {
            rpc.delete(handle, p);
        }
    }
    rpc_list.leave();
}
