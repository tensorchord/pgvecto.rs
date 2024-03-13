#![feature(trait_alias)]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512))]

mod instance;
mod version;
mod worker;

pub use version::Version;
pub use worker::Worker;
