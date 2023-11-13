use super::gucs::Transport;
use super::gucs::TRANSPORT;
use crate::ipc::client::Rpc;
use crate::ipc::{connect_mmap, connect_unix};
use crate::prelude::*;
use std::cell::RefCell;
use std::collections::BTreeSet;

#[thread_local]
static FLUSH_IF_COMMIT: RefCell<BTreeSet<Id>> = RefCell::new(BTreeSet::new());

#[thread_local]
static DROP_IF_COMMIT: RefCell<BTreeSet<Id>> = RefCell::new(BTreeSet::new());

#[thread_local]
static CLIENT: RefCell<Option<Rpc>> = RefCell::new(None);

pub fn aborting() {
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
    *DROP_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn committing() {
    {
        let flush_if_commit = FLUSH_IF_COMMIT.borrow();
        let drop_if_commit = DROP_IF_COMMIT.borrow();
        if flush_if_commit.len() != 0 || drop_if_commit.len() != 0 {
            client(|mut rpc| {
                for id in flush_if_commit.iter().copied() {
                    rpc.flush(id).unwrap();
                }

                for id in drop_if_commit.iter().copied() {
                    rpc.destory(id).unwrap();
                }

                rpc
            });
        }
    }
    *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
    *DROP_IF_COMMIT.borrow_mut() = BTreeSet::new();
}

pub fn drop_if_commit(id: Id) {
    DROP_IF_COMMIT.borrow_mut().insert(id);
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
