use crate::index::hook_transaction::flush_if_commit;
use crate::prelude::*;
use service::prelude::*;

pub fn update_insert(id: Id, vector: DynamicVector, tid: pgrx::pg_sys::ItemPointerData) {
    flush_if_commit(id);
    let p = Pointer::from_sys(tid);
    let mut client = super::client::borrow_mut();
    client.insert(id, (vector, p));
}

pub fn update_delete(id: Id, hook: impl Fn(Pointer) -> bool) {
    struct ClientDelete<H> {
        hook: H,
    }

    impl<H> crate::ipc::client::ClientDelete for ClientDelete<H>
    where
        H: Fn(Pointer) -> bool,
    {
        fn test(&mut self, p: Pointer) -> bool {
            (self.hook)(p)
        }
    }

    let client_delete = ClientDelete { hook };

    flush_if_commit(id);
    let mut client = super::client::borrow_mut();
    client.delete(id, client_delete);
}
