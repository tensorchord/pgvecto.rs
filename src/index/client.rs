use crate::gucs::{Transport, TRANSPORT};
use crate::ipc::client::Client;
use crate::utils::cells::PgRefCell;
use std::cell::RefMut;
use std::ops::{Deref, DerefMut};

static CLIENT: PgRefCell<Option<Client>> = unsafe { PgRefCell::new(None) };

pub fn borrow_mut() -> ClientGuard {
    let mut x = CLIENT.borrow_mut();
    if x.is_none() {
        *x = Some(match TRANSPORT.get() {
            Transport::unix => crate::ipc::connect_unix(),
            Transport::mmap => crate::ipc::connect_mmap(),
        });
    }
    ClientGuard(x)
}

pub struct ClientGuard(RefMut<'static, Option<Client>>);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.take();
        }
    }
}

impl Deref for ClientGuard {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl DerefMut for ClientGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}
