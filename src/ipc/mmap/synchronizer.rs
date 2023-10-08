use crate::ipc::{ChannelTrait, ChannelWithSerialize};
use mmap_sync::{instance::InstanceVersion, synchronizer::Synchronizer};
use std::{error::Error, time::Duration};

pub struct MmapSynchronizer {
    reader: Synchronizer,
    writer: Synchronizer,
    reader_state: InstanceVersion,
}

impl MmapSynchronizer {
    pub fn conn() -> Self {
        let mut connector = Self::new("./pg_vectors/_mmap_listen", false);
        connector.write(&vec![0u8; 1]).expect("Failed to write.");
        let num: usize = (&mut connector as &mut dyn ChannelTrait)
            .recv()
            .expect("Failed to recv.");
        let path = format!("./pg_vectors/_mmap_{num}");
        Self::new(&path, false)
    }

    pub fn new(path: &str, is_server: bool) -> Self {
        // The filename represents which side reads it.
        let mut reader_path = path.to_string();
        let mut writer_path = path.to_string();
        if is_server {
            reader_path.push_str("_server");
            writer_path.push_str("_client");
        } else {
            writer_path.push_str("_client");
            reader_path.push_str("_server");
        }
        let mut reader = Synchronizer::new(reader_path.as_ref());
        let writer = Synchronizer::new(writer_path.as_ref());

        // The reader need to write something first to initialize the mmap file and state.
        reader
            .write(&[0u8; 1], Duration::from_secs(1))
            .expect("Failed to write.");
        unsafe { reader.read::<[u8; 1]>(false) }.expect("Failed to read.");
        let reader_state = reader.version().expect("Failed to get reader state.");
        Self {
            reader,
            writer,
            reader_state,
        }
    }
}

impl ChannelTrait for MmapSynchronizer {
    fn write(&mut self, buf: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.writer.write(buf, Duration::from_secs(1))?;
        Ok(())
    }

    fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut state;
        loop {
            state = self.reader.version()?;
            if state != self.reader_state {
                break;
            }
            std::thread::yield_now();
        }
        let buffer = unsafe { self.reader.read::<Vec<u8>>(true) }?.to_vec();
        self.reader_state = state;
        Ok(buffer)
    }
}
