use super::find_index;
use super::index::Index;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    io::ErrorKind,
    time::{Duration, Instant},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientPacket {
    // requests
    Build0 {
        id: Id,
        options: Options,
    },
    Build1((Box<[Scalar]>, Pointer)),
    Build2,
    Load {
        id: Id,
    },
    Unload {
        id: Id,
    },
    Insert {
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
    },
    Delete {
        id: Id,
        delete: Pointer,
    },
    Search {
        id: Id,
        search: (Box<[Scalar]>, usize),
    },
    Flush {
        id: Id,
    },
    Drop {
        id: Id,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerPacket {
    Reset(String),
    // responses
    Build {},
    Load {},
    Unload {},
    Insert {},
    Delete {},
    Search { result: Vec<Pointer> },
    Flush {},
    Drop {},
}

struct Server {
    read: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    write: tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl Server {
    fn new(stream: tokio::net::TcpStream) -> Self {
        let (read, write) = stream.into_split();
        let read = tokio::io::BufReader::new(read);
        let write = tokio::io::BufWriter::new(write);
        Self { read, write }
    }
    async fn recv(&mut self) -> anyhow::Result<ClientPacket> {
        use tokio::io::AsyncReadExt;
        let packet_size = self.read.read_u16().await?;
        let mut buffer = vec![0u8; packet_size as usize];
        self.read.read_exact(&mut buffer).await?;
        buffer.deserialize()
    }
    async fn send(&mut self, maybe: anyhow::Result<ServerPacket>) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let packet = match maybe {
            Ok(packet) => packet,
            Err(e) => ServerPacket::Reset(e.to_string()),
        };
        let packet = packet.bincode()?;
        anyhow::ensure!(packet.len() <= u16::MAX as usize);
        let packet_size = packet.len() as u16;
        self.write.write_u16(packet_size).await?;
        self.write.write_all(&packet).await?;
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        self.write.flush().await?;
        Ok(())
    }
}

pub async fn server_main(stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut server = Server::new(stream);
    loop {
        let packet = server.recv().await?;
        match packet {
            ClientPacket::Build0 { id, options } => {
                let (tx, rx) = async_channel::bounded(65536);
                let maybe = {
                    let data = tokio::spawn(async move {
                        loop {
                            let packet = server.recv().await?;
                            match packet {
                                ClientPacket::Build1(data) => {
                                    tx.send(data).await?;
                                }
                                ClientPacket::Build2 => {
                                    drop(tx);
                                    return anyhow::Result::Ok(server);
                                }
                                _ => anyhow::bail!("Bad state."),
                            }
                        }
                    });
                    handler_build(id, options, rx).await?;
                    server = data.await??;
                    Ok(ServerPacket::Build {})
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Load { id } => {
                let maybe = async {
                    handler_load(id).await?;
                    Ok(ServerPacket::Load {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Unload { id } => {
                let maybe = async {
                    handler_unload(id).await?;
                    Ok(ServerPacket::Unload {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Insert { id, insert } => {
                let maybe = async {
                    handler_insert(id, insert).await?;
                    Ok(ServerPacket::Insert {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Delete { id, delete } => {
                let maybe = async {
                    handler_delete(id, delete).await?;
                    Ok(ServerPacket::Delete {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Search { id, search } => {
                let maybe = async {
                    let data = handler_search(id, search).await?;
                    Ok(ServerPacket::Search { result: data })
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Flush { id } => {
                let maybe = async {
                    handler_flush(id).await?;
                    Ok(ServerPacket::Flush {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Drop { id } => {
                let maybe = async {
                    handler_drop(id).await?;
                    Ok(ServerPacket::Drop {})
                }
                .await;
                server.send(maybe).await?;
                server.flush().await?;
            }
            _ => anyhow::bail!("Bad state."),
        }
    }
}

async fn handler_build(
    id: Id,
    options: Options,
    data: async_channel::Receiver<(Box<[Scalar]>, Pointer)>,
) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let mut guard = index.write().await;
    if guard.is_unloaded() {
        guard.load(Index::build(id, options, data).await?);
    }
    Ok(())
}

async fn handler_load(id: Id) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let mut guard = index.write().await;
    if guard.is_unloaded() {
        guard.load(Index::load(id).await?);
    }
    Ok(())
}

async fn handler_unload(id: Id) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let mut guard = index.write().await;
    if guard.is_loaded() {
        guard.unload().shutdown().await?;
    }
    Ok(())
}

async fn handler_insert(id: Id, insert: (Box<[Scalar]>, Pointer)) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let index = index.read().await;
    let index = index.get()?;
    index.insert(insert).await?;
    Ok(())
}

async fn handler_delete(id: Id, delete: Pointer) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let index = index.read().await;
    let index = index.get()?;
    index.delete(delete).await?;
    Ok(())
}

async fn handler_search(
    id: Id,
    (x_vector, k): (Box<[Scalar]>, usize),
) -> anyhow::Result<Vec<Pointer>> {
    let index = find_index(id).await?;
    let index = index.read().await;
    let index = index.get()?;
    let data = index.search((x_vector, k)).await?;
    Ok(data)
}

async fn handler_flush(id: Id) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let index = index.read().await;
    let index = index.get()?;
    index.flush().await?;
    Ok(())
}

async fn handler_drop(id: Id) -> anyhow::Result<()> {
    let index = find_index(id).await?;
    let mut guard = index.write().await;
    if guard.is_loaded() {
        let x = guard.unload();
        x.shutdown().await?;
        Index::drop(id).await?;
    }
    Ok(())
}

pub struct Client {
    read: std::io::BufReader<std::net::TcpStream>,
    write: std::io::BufWriter<std::net::TcpStream>,
}

impl Client {
    pub fn new(tcp: std::net::TcpStream) -> anyhow::Result<Self> {
        let read = std::io::BufReader::new(tcp.try_clone()?);
        let write = std::io::BufWriter::new(tcp.try_clone()?);
        Ok(Self { read, write })
    }

    fn _recv(&mut self) -> anyhow::Result<ServerPacket> {
        use byteorder::BigEndian as E;
        use byteorder::ReadBytesExt;
        use std::io::Read;
        let packet_size = self.read.read_u16::<E>()?;
        let mut buffer = vec![0u8; packet_size as usize];
        self.read.read_exact(&mut buffer)?;
        buffer.deserialize()
    }

    fn _send(&mut self, packet: ClientPacket) -> anyhow::Result<()> {
        use byteorder::BigEndian as E;
        use byteorder::WriteBytesExt;
        use std::io::Write;
        let packet = packet.bincode()?;
        anyhow::ensure!(packet.len() <= u16::MAX as usize);
        let packet_size = packet.len() as u16;
        self.write.write_u16::<E>(packet_size)?;
        self.write.write_all(&packet)?;
        Ok(())
    }

    fn _test(&mut self) -> anyhow::Result<bool> {
        if !self.read.buffer().is_empty() {
            return Ok(true);
        }
        unsafe {
            use std::os::fd::AsRawFd;
            let mut buf = [0u8];
            let result = libc::recv(
                self.read.get_mut().as_raw_fd(),
                buf.as_mut_ptr() as _,
                1,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            );
            match result {
                -1 => {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == ErrorKind::WouldBlock {
                        Ok(false)
                    } else {
                        Err(err.into())
                    }
                }
                0 => {
                    // TCP stream is closed.
                    Ok(false)
                }
                1 => Ok(true),
                _ => unreachable!(),
            }
        }
    }

    fn _flush(&mut self) -> anyhow::Result<()> {
        use std::io::Write;
        self.write.flush()?;
        Ok(())
    }

    pub fn build(&mut self, id: Id, options: Options) -> anyhow::Result<ClientBuild> {
        self._send(ClientPacket::Build0 { id, options })?;
        Ok(ClientBuild {
            last: Instant::now(),
            client: self,
        })
    }

    pub fn insert(&mut self, id: Id, insert: (Box<[Scalar]>, Pointer)) -> anyhow::Result<()> {
        self._send(ClientPacket::Insert { id, insert })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Insert {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn delete(&mut self, id: Id, delete: Pointer) -> anyhow::Result<()> {
        self._send(ClientPacket::Delete { id, delete })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Delete {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn search(
        &mut self,
        id: Id,
        search: (Box<[Scalar]>, usize),
    ) -> anyhow::Result<Vec<Pointer>> {
        self._send(ClientPacket::Search { id, search })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Search { result } => Ok(result),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn load(&mut self, id: Id) -> anyhow::Result<()> {
        self._send(ClientPacket::Load { id })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Load {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn unload(&mut self, id: Id) -> anyhow::Result<()> {
        self._send(ClientPacket::Unload { id })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Unload {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn flush(&mut self, id: Id) -> anyhow::Result<()> {
        self._send(ClientPacket::Flush { id })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Flush {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }

    pub fn drop(&mut self, id: Id) -> anyhow::Result<()> {
        self._send(ClientPacket::Drop { id })?;
        self._flush()?;
        let packet = self._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Drop {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }
}

pub struct ClientBuild<'a> {
    last: Instant,
    client: &'a mut Client,
}

impl<'a> ClientBuild<'a> {
    fn _process(&mut self) -> anyhow::Result<()> {
        if self.last.elapsed() > Duration::from_millis(200) {
            while self.client._test()? {
                let packet = self.client._recv()?;
                match packet {
                    ServerPacket::Reset(e) => anyhow::bail!(e),
                    _ => anyhow::bail!("Bad state."),
                }
            }
            self.last = Instant::now();
        }
        Ok(())
    }
    pub fn next(&mut self, data: (Box<[Scalar]>, Pointer)) -> anyhow::Result<()> {
        self._process()?;
        self.client._send(ClientPacket::Build1(data))?;
        Ok(())
    }
    pub fn finish(self) -> anyhow::Result<()> {
        self.client._send(ClientPacket::Build2)?;
        self.client._flush()?;
        let packet = self.client._recv()?;
        match packet {
            ServerPacket::Reset(e) => anyhow::bail!(e),
            ServerPacket::Build {} => Ok(()),
            _ => anyhow::bail!("Bad state."),
        }
    }
}
