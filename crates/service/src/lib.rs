#![allow(clippy::needless_range_loop)]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512))]

mod algorithms;
mod index;
mod instance;
mod prelude;
mod storage;
mod utils;
mod worker;

pub use worker::Worker;
