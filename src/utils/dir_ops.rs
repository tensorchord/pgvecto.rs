use std::fs::File;
use std::path::Path;

pub fn sync_dir(path: impl AsRef<Path>) {
    let file = File::open(path).expect("Failed to sync dir.");
    file.sync_all().expect("Failed to sync dir.");
}
