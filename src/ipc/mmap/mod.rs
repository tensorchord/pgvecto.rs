mod synchronizer;

pub use self::synchronizer::MmapSynchronizer;
use super::{ChannelTrait, ChannelWithSerialize};

pub struct Listener {
    channel: MmapSynchronizer,
    num: usize,
}

impl Listener {
    pub fn new() -> Self {
        let path = "./_mmap_listen";
        let channel = MmapSynchronizer::new(path.as_ref(), true);

        Self { channel, num: 0 }
    }

    pub fn accept(&mut self) -> MmapSynchronizer {
        self.channel.read().expect("Failed to read.");
        (&mut self.channel as &mut dyn ChannelTrait)
            .send(self.num)
            .expect("Failed to send.");
        self.num += 1;

        let path = format!("./_mmap_{}", self.num);
        MmapSynchronizer::new(path.as_ref(), true)
    }
}
