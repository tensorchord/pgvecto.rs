use std::fs::read_dir;
use std::io;
use std::path::Path;

pub fn dir_size(dir: &Path) -> io::Result<u64> {
    let mut size = 0;
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().unwrap().to_string_lossy();
            if name.starts_with('.') {
                // ignore hidden files
                continue;
            }
            if path.is_dir() {
                size += dir_size(&path)?;
            } else {
                size += entry.metadata()?.len();
            }
        }
    }
    Ok(size)
}
