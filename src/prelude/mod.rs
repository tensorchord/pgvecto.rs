mod distance;
mod scalar;
mod sys;

pub use self::distance::Distance;
pub use self::scalar::{Float, Scalar};
pub use self::sys::{Id, Pointer};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum Memmap {
    Ram = 0,
    Disk = 1,
}
