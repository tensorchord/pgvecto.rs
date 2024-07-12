use base::index::SearchOptions;
use base::search::Pointer;
use base::worker::ViewBasicOperations;
use service::Instance;

use log::{info, warn};
use std::cmp::min;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::args::{Arguments, SubCommandEnum};
use crate::read::{convert_to_owned_vec, read_vectors};

mod args;
mod read;

const INTERVAL: Duration = Duration::from_secs(1);

fn default_search_opt() -> SearchOptions {
    SearchOptions {
        hnsw_ef_search: 100,
        ivf_nprobe: 10,
        diskann_ef_search: 100,
    }
}

fn calculate_precision(truth: &Vec<i32>, res: &Vec<i32>, top: usize) -> f32 {
    let mut count = 0;
    let length = min(top, truth.len());
    for i in 0..length {
        for j in 0..min(length, res.len()) {
            if res[j] == truth[i] {
                count += 1;
                break;
            }
        }
    }
    (count as f32) / (length as f32)
}

fn main() {
    let args: Arguments = argh::from_env();
    let path = PathBuf::from_str(&args.path).expect("failed to parse the path");
    let mut log_builder = env_logger::builder();
    if args.verbose {
        log_builder.filter_level(log::LevelFilter::Debug);
    } else {
        log_builder.filter_level(log::LevelFilter::Info);
    }
    log_builder.init();

    match args.cmd {
        SubCommandEnum::Add(add) => {
            let instance = Instance::open(path);
            instance.refresh();
            let dim = instance.stat().options.vector.dims as usize;
            let mut count = 0;
            let path = PathBuf::from_str(&add.file).expect("failed to parse the file path");
            let vectors = read_vectors::<f32>(&path).expect("failed to read vec from file");
            let mut view = instance.view();
            let mut i = 0;
            while i < vectors.len() {
                if vectors[i].len() != (dim) {
                    let length = vectors[i].len();
                    warn!("found unmatched vector dim: {length}!={dim}");
                    continue;
                }
                let owned_vec = convert_to_owned_vec(&vectors[i]);
                let pointer = Pointer::new(count as u64);
                match view.insert(owned_vec, pointer) {
                    Ok(res) => {
                        if res.is_err() {
                            info!("refresh the instance to insert vector {i}");
                            instance.refresh();
                            view = instance.view();
                            continue;
                        }
                        i += 1;
                        count += 1;
                    }
                    Err(err) => {
                        warn!("failed to insert: {err}");
                        i += 1
                    }
                }
            }
            std::mem::forget(instance);
            info!("{count} records have been added to the index");
        }
        SubCommandEnum::Build(build) => {
            let instance = Instance::open(path);
            if let Some(num) = build.threads {
                if let Err(err) = instance.alter("optimizing.optimizing_threads", &num.to_string())
                {
                    warn!("failed to alter the optimizing thread: {err}");
                }
            }
            let timeout = Duration::from_secs(build.timeout_seconds);
            instance.start();
            let start_time = Instant::now();
            loop {
                if !instance.stat().indexing {
                    break;
                };
                if start_time.elapsed() > timeout {
                    warn!("force stop the instance due to timeout");
                    instance.stop();
                    break;
                }
                std::thread::sleep(INTERVAL);
            }
            std::mem::forget(instance);
            info!("index has been built/optimized");
        }
        SubCommandEnum::Create(create) => {
            let (index_options, alterable_options) = create
                .get_index_options()
                .expect("failed to parse create arguments");
            fs::create_dir_all(&path.parent().expect("failed to get the parent path"))
                .expect("failed to create the parent path");
            let instance = Instance::create(path, index_options, alterable_options)
                .expect("failed to create instance");
            std::mem::forget(instance);
            info!("index has been saved");
        }
        SubCommandEnum::Query(query) => {
            let instance = Instance::open(path);
            let query_file =
                PathBuf::from_str(&query.query).expect("failed to parse the query file path");
            let truth_file =
                PathBuf::from_str(&query.truth).expect("failed to parse the truth file path");
            let queries = read_vectors::<f32>(&query_file).expect("failed to read the query file");
            let truth = read_vectors::<i32>(&truth_file).expect("failed to read the truth file");
            let mut res = Vec::with_capacity(queries.len());
            let view = instance.view();
            std::mem::forget(instance);
            let search_opt = default_search_opt();
            for (i, vec) in queries.iter().enumerate() {
                match view.basic(&convert_to_owned_vec(&vec), &search_opt) {
                    Ok(iter) => {
                        let ans = iter.map(|(_, x)| x.as_u64() as i32).collect();
                        res.push(calculate_precision(&truth[i], &ans, query.top_k));
                    }
                    Err(err) => {
                        info!("failed to search the vector: {err}");
                    }
                }
            }
            info!(
                "Top {} precision of {} queries is {}",
                query.top_k,
                res.len(),
                res.iter().sum::<f32>() / (res.len() as f32)
            );
        }
    }
}
