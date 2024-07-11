use std::fs::File;
use std::path::Path;

pub fn sync_dir(path: impl AsRef<Path>) {
    let file = File::open(path).expect("Failed to sync dir.");
    file.sync_all().expect("Failed to sync dir.");
}

pub fn sync_walk_from_dir(path: impl AsRef<Path>) {
    let entries = std::fs::read_dir(&path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for entry in entries {
        let t = entry.file_type().unwrap();
        if t.is_symlink() {
            panic!("there should not be symlinks");
        } else if t.is_dir() {
            sync_walk_from_dir(entry.path());
        } else if t.is_file() {
            let file = File::open(entry.path()).expect("Failed to sync file.");
            file.sync_all().expect("Failed to sync file.");
        } else {
            panic!("unknown file type");
        }
    }
    let dir = File::open(path).expect("Failed to sync file.");
    dir.sync_all().expect("Failed to sync file.");
}
