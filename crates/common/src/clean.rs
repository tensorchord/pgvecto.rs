use std::collections::HashSet;
use std::path::Path;

pub fn clean(path: impl AsRef<Path>, wanted: impl Iterator<Item = String>) {
    let dirs = std::fs::read_dir(&path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let wanted = HashSet::<String>::from_iter(wanted);
    for dir in dirs {
        let filename = dir.file_name();
        let filename = filename.to_str().unwrap();
        let p = path.as_ref().join(filename);
        if !wanted.contains(filename) {
            log::info!("Delete outdated directory {:?}.", p);
            std::fs::remove_dir_all(p).unwrap();
        } else {
            log::info!("Find directory {:?}.", p);
        }
    }
}
