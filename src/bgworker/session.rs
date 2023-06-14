use super::find_index;
use super::index::Index;
use crate::prelude::{BincodeDeserialize, BincodeSerialize, Error, Options};
use crate::prelude::{Id, Pointer, Scalar};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ClientPacket {
    // requests
    Build0 {
        id: Id,
        options: Options,
    },
    Build1 {
        data: (Vec<Scalar>, Pointer),
    },
    Build2,
    Load {
        id: Id,
    },
    Unload {
        id: Id,
    },
    Insert {
        id: Id,
        insert: (Vec<Scalar>, Pointer),
    },
    Delete {
        id: Id,
        delete: Pointer,
    },
    Search {
        id: Id,
        search: (Vec<Scalar>, usize),
    },
    Flush {
        id: Id,
    },
    Drop {
        id: Id,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ServerPacket {
    Reset(Error),
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
        let packet = buffer.deserialize()?;
        Ok(packet)
    }
    async fn send(&mut self, maybe: anyhow::Result<ServerPacket>) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let packet = match maybe {
            Ok(packet) => packet,
            Err(e) => match e.downcast::<Error>() {
                Ok(e) => ServerPacket::Reset(e),
                Err(e) => anyhow::bail!(e),
            },
        };
        let packet = packet.serialize()?;
        anyhow::ensure!(packet.len() <= u16::MAX as usize);
        let packet_size = packet.len() as u16;
        self.write.write_u16(packet_size).await?;
        self.write.write(&packet).await?;
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
                                ClientPacket::Build1 { data } => {
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
                let maybe = try {
                    handler_load(id).await?;
                    ServerPacket::Load {}
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Unload { id } => {
                let maybe = try {
                    handler_unload(id).await?;
                    ServerPacket::Unload {}
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Insert { id, insert } => {
                let maybe = try {
                    handler_insert(id, insert).await?;
                    ServerPacket::Insert {}
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Delete { id, delete } => {
                let maybe = try {
                    handler_delete(id, delete).await?;
                    ServerPacket::Delete {}
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Search { id, search } => {
                let maybe = try {
                    let data = handler_search(id, search).await?;
                    ServerPacket::Search { result: data }
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Flush { id } => {
                let maybe = try {
                    handler_flush(id).await?;
                    ServerPacket::Flush {}
                };
                server.send(maybe).await?;
                server.flush().await?;
            }
            ClientPacket::Drop { id } => {
                let maybe = try {
                    handler_drop(id).await?;
                    ServerPacket::Drop {}
                };
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
    data: async_channel::Receiver<(Vec<Scalar>, Pointer)>,
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

async fn handler_insert(id: Id, insert: (Vec<Scalar>, Pointer)) -> anyhow::Result<()> {
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

async fn handler_search(id: Id, search: (Vec<Scalar>, usize)) -> anyhow::Result<Vec<Pointer>> {
    let index = find_index(id).await?;
    let index = index.read().await;
    let index = index.get()?;
    let data = index.search(search).await?;
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

struct ClientInner {
    read: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    write: tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl ClientInner {
    fn new(stream: tokio::net::TcpStream) -> Self {
        let (read, write) = stream.into_split();
        let read = tokio::io::BufReader::new(read);
        let write = tokio::io::BufWriter::new(write);
        Self { read, write }
    }
    async fn recv(
        read: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    ) -> anyhow::Result<ServerPacket> {
        use tokio::io::AsyncReadExt;
        let packet_size = read.read_u16().await?;
        let mut buffer = vec![0u8; packet_size as usize];
        read.read_exact(&mut buffer).await?;
        Ok(buffer.deserialize()?)
    }
    async fn send(
        write: &mut tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
        packet: ClientPacket,
    ) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let packet = packet.serialize()?;
        anyhow::ensure!(packet.len() <= u16::MAX as usize);
        let packet_size = packet.len() as u16;
        write.write_u16(packet_size).await?;
        write.write(&packet).await?;
        Ok(())
    }
    async fn flush(
        write: &mut tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    ) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        write.flush().await?;
        Ok(())
    }
}

pub struct Client {
    runtime: tokio::runtime::Runtime,
    inner: ClientInner,
}

impl Client {
    pub fn new(stream: std::net::TcpStream) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let stream = runtime.block_on(async move { tokio::net::TcpStream::from_std(stream) })?;
        let inner = ClientInner::new(stream);
        Ok(Self { runtime, inner })
    }

    pub fn build(
        &mut self,
        id: Id,
        options: Options,
        data: async_channel::Receiver<(Vec<Scalar>, Pointer)>,
    ) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Build0 { id, options }).await?;
            while let Ok(data) = data.recv().await {
                ClientInner::send(&mut inner.write, ClientPacket::Build1 { data }).await?;
            }
            ClientInner::send(&mut inner.write, ClientPacket::Build2).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Build {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn insert(&mut self, id: Id, insert: (Vec<Scalar>, Pointer)) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Insert { id, insert }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Insert {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn delete(&mut self, id: Id, delete: Pointer) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Delete { id, delete }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Delete {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn search(&mut self, id: Id, search: (Vec<Scalar>, usize)) -> anyhow::Result<Vec<Pointer>> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Search { id, search }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Search { result } => Ok(result),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn load(&mut self, id: Id) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Load { id }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Load {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn unload(&mut self, id: Id) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Unload { id }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Unload {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn flush(&mut self, id: Id) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Flush { id }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Flush {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
    pub fn drop(&mut self, id: Id) -> anyhow::Result<()> {
        let inner = &mut self.inner;
        self.runtime.block_on(async move {
            ClientInner::send(&mut inner.write, ClientPacket::Drop { id }).await?;
            ClientInner::flush(&mut inner.write).await?;
            let packet = ClientInner::recv(&mut inner.read).await?;
            match packet {
                ServerPacket::Reset(e) => anyhow::bail!(e),
                ServerPacket::Drop {} => Ok(()),
                _ => anyhow::bail!("Bad state."),
            }
        })
    }
}
