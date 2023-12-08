use super::dir_ops::sync_dir;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct FileAtomic<T> {
    path: PathBuf,
    data: T,
}

impl<T> FileAtomic<T> {
    pub fn get(&self) -> &T {
        &self.data
    }
}

impl<T> FileAtomic<T>
where
    T: serde::Serialize,
{
    pub fn create(path: impl AsRef<Path>, data: T) -> Self {
        std::fs::create_dir(&path).unwrap();
        let path = path.as_ref().to_owned();
        write(path.join("0"), &data);
        sync_dir(&path);
        Self { path, data }
    }
    pub fn set(&mut self, data: T) {
        write(self.path.join("1"), &data);
        sync_dir(&self.path);
        {
            let dir = File::open(&self.path).unwrap();
            rustix::fs::renameat(&dir, "1", &dir, "0").unwrap();
        }
        sync_dir(&self.path);
        self.data = data;
    }
}

impl<T> FileAtomic<T>
where
    T: for<'a> serde::Deserialize<'a>,
{
    pub fn open(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_owned();
        if path.join("1").try_exists().unwrap() {
            std::fs::remove_file(path.join("1")).unwrap();
            sync_dir(&path);
        }
        let contents = std::fs::read_to_string(path.join("0")).unwrap();
        let data = serde_json::from_str(&contents).unwrap();
        Self { path, data }
    }
}

fn write<T>(path: impl AsRef<Path>, contents: &T)
where
    T: serde::Serialize,
{
    let contents = serde_json::to_string(contents).unwrap();
    let file = File::options()
        .create_new(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    (&file).write_all(contents.as_bytes()).unwrap();
    file.sync_all().unwrap();
}
