mod socket;

pub use self::socket::Socket;
use std::{io::ErrorKind, os::unix::net::UnixListener, path::Path};

pub struct Listener {
    listener: UnixListener,
}

impl Listener {
    pub fn new() -> Self {
        let path = "./_socket";
        remove_file_if_exists(&path).expect("Failed to bind.");
        let listener = UnixListener::bind(&path).expect("Failed to bind.");
        Self { listener }
    }
    pub fn accept(&mut self) -> Socket {
        let (stream, _) = self.listener.accept().expect("Failed to listen.");
        Socket {
            stream: Some(stream),
        }
    }
}

fn remove_file_if_exists(path: impl AsRef<Path>) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
