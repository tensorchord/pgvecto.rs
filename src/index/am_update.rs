use crate::index::hook_transaction::callback_dirty;
use crate::prelude::*;
use service::prelude::*;

pub fn update_insert(handle: Handle, vector: DynamicVector, tid: pgrx::pg_sys::ItemPointerData) {
    callback_dirty(handle);

    let pointer = Pointer::from_sys(tid);
    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.insert(handle, vector, pointer);
}

pub fn update_delete(handle: Handle, hook: impl Fn(Pointer) -> bool) {
    callback_dirty(handle);

    struct Delete<H> {
        hook: H,
    }

    impl<H> crate::ipc::client::Delete for Delete<H>
    where
        H: Fn(Pointer) -> bool,
    {
        fn test(&mut self, p: Pointer) -> bool {
            (self.hook)(p)
        }
    }

    let client_delete = Delete { hook };

    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.delete(handle, client_delete);
}
