use super::gucs::Transport;
use super::gucs::TRANSPORT;
use crate::ipc::client::Rpc;
use crate::ipc::{connect_mmap, connect_unix};
use crate::prelude::*;
use crate::utils::cells::PgRefCell;
use std::cell::RefMut;
use std::collections::BTreeSet;

static FLUSH_IF_COMMIT: PgRefCell<BTreeSet<Id>> = unsafe { PgRefCell::new(BTreeSet::new()) };

static CLIENT: PgRefCell<Option<Rpc>> = unsafe { PgRefCell::new(None) };

pub fn aborting() {
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn committing() {
    {
        let flush_if_commit = FLUSH_IF_COMMIT.borrow();
        if flush_if_commit.len() != 0 {
            client(|mut rpc| {
                for id in flush_if_commit.iter().copied() {
                    rpc.flush(id).friendly().friendly();
                }

                rpc
            });
        }
    }
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn flush_if_commit(id: Id) {
    FLUSH_IF_COMMIT.borrow_mut().insert(id);
}

pub fn client<F>(f: F)
where
    F: FnOnce(Rpc) -> Rpc,
{
    let mut guard = CLIENT.borrow_mut();
    let client = guard.take().unwrap_or_else(|| match TRANSPORT.get() {
        Transport::unix => connect_unix(),
        Transport::mmap => connect_mmap(),
    });
    let client = f(client);
    *guard = Some(client);
}

pub struct ClientGuard(RefMut<'static, Option<Rpc>>);

pub fn client_guard() -> (Rpc, ClientGuard) {
    let mut guard = CLIENT.borrow_mut();
    let client = guard.take().unwrap_or_else(|| match TRANSPORT.get() {
        Transport::unix => connect_unix(),
        Transport::mmap => connect_mmap(),
    });
    (client, ClientGuard(guard))
}

impl ClientGuard {
    pub fn reset(mut self, client: Rpc) {
        *self.0 = Some(client);
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        if self.0.is_none() {
            panic!("ClientGuard was dropped without resetting the client");
        }
    }
}
