#![allow(clippy::needless_range_loop)]
#![feature(stdsimd)]

mod algorithms;
mod index;
mod instance;
mod prelude;
mod storage;
mod utils;
mod worker;

pub use worker::Worker;
