#![feature(const_float_bits_conv)]
#![feature(avx512_target_feature)]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512))]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512_f16))]
#![allow(clippy::derivable_impls)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::len_zero)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::nonminimal_bool)]

pub mod aligned;
pub mod always_equal;
pub mod distance;
pub mod index;
pub mod operator;
pub mod pod;
pub mod rand;
pub mod scalar;
pub mod search;
pub mod vector;
pub mod worker;
