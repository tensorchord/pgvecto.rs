use crate::index::hook_transaction::flush_if_commit;
use crate::prelude::*;
use service::prelude::*;

pub fn update_insert(handle: Handle, vector: DynamicVector, tid: pgrx::pg_sys::ItemPointerData) {
    flush_if_commit(handle);
    let p = Pointer::from_sys(tid);
    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.insert(handle, (vector, p));
}

pub fn update_delete(handle: Handle, hook: impl Fn(Pointer) -> bool) {
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

    flush_if_commit(handle);
    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.delete(handle, client_delete);
}
