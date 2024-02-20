use crate::index::hook_transaction::callback_dirty;
use crate::prelude::*;

pub fn update_insert(handle: Handle, vector: OwnedVector, tid: pgrx::pg_sys::ItemPointerData) {
    callback_dirty(handle);

    let pointer = Pointer::from_sys(tid);
    let mut rpc = check_client(crate::ipc::client());

    match rpc.insert(handle, vector, pointer) {
        Ok(()) => (),
        Err(InsertError::NotExist) => bad_service_not_exist(),
        Err(InsertError::Upgrade) => bad_service_upgrade(),
        Err(InsertError::InvalidVector) => bad_service_invalid_vector(),
    }
}

pub fn update_delete(handle: Handle, f: impl Fn(Pointer) -> bool) {
    callback_dirty(handle);

    let mut rpc_list = match check_client(crate::ipc::client()).list(handle) {
        Ok(x) => x,
        Err((_, ListError::NotExist)) => bad_service_not_exist(),
        Err((_, ListError::Upgrade)) => bad_service_upgrade(),
    };
    let mut rpc = check_client(crate::ipc::client());
    while let Some(p) = rpc_list.next() {
        if f(p) {
            match rpc.delete(handle, p) {
                Ok(()) => (),
                Err(DeleteError::NotExist) => (),
                Err(DeleteError::Upgrade) => (),
            }
        }
    }
    rpc_list.leave();
}
