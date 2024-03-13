use crate::error::*;
use crate::index::hook_maintain::maintain_index_in_index_access;
use crate::utils::sys::FromSys;
use base::index::*;
use base::search::*;
use base::vector::*;

pub fn update_insert(handle: Handle, vector: OwnedVector, tid: pgrx::pg_sys::ItemPointerData) {
    maintain_index_in_index_access(handle);

    let pointer = Pointer::from_sys(tid);
    let mut rpc = check_client(crate::ipc::client());

    match rpc.insert(handle, vector, pointer) {
        Ok(()) => (),
        Err(InsertError::NotExist) => bad_service_not_exist(),
        Err(InsertError::InvalidVector) => bad_service_invalid_vector(),
    }
}

pub fn update_delete(handle: Handle, f: impl Fn(Pointer) -> bool) {
    maintain_index_in_index_access(handle);

    let mut rpc_list = match check_client(crate::ipc::client()).list(handle) {
        Ok(x) => x,
        Err((_, ListError::NotExist)) => bad_service_not_exist(),
    };
    let mut rpc = check_client(crate::ipc::client());
    while let Some(p) = rpc_list.next() {
        if f(p) {
            match rpc.delete(handle, p) {
                Ok(()) => (),
                Err(DeleteError::NotExist) => (),
            }
        }
    }
    rpc_list.leave();
}
