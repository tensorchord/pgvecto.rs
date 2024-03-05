#![feature(core_intrinsics)]
#![feature(avx512_target_feature)]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512))]
#![allow(internal_features)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::len_zero)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::nonminimal_bool)]

pub mod distance;
pub mod error;
pub mod global;
pub mod index;
pub mod scalar;
pub mod search;
pub mod vector;
pub mod worker;
