use std::path::Path;

pub fn variants<const N: usize>(path: impl AsRef<Path>, variants: [&str; N]) -> &str {
    let dir = std::fs::read_dir(path).expect("failed to read dir");
    let files = dir
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to walk dir");
    let mut matches = vec![];
    for file in files {
        for name in variants {
            if file.file_name() == *name {
                matches.push(name);
            }
        }
    }
    if matches.len() > 1 {
        panic!("too many matches");
    }
    if matches.is_empty() {
        panic!("no matches");
    }
    matches[0]
}
