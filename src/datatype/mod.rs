pub mod aggregate_svecf32;
pub mod aggregate_vecf32;
pub mod binary;
pub mod binary_bvecf32;
pub mod binary_svecf32;
pub mod binary_vecf16;
pub mod binary_vecf32;
pub mod binary_veci8;
pub mod casts;
pub mod functions_bvecf32;
pub mod functions_svecf32;
pub mod functions_vecf16;
pub mod functions_vecf32;
pub mod functions_veci8;
pub mod memory_bvecf32;
pub mod memory_svecf32;
pub mod memory_vecf16;
pub mod memory_vecf32;
pub mod memory_veci8;
pub mod operators_bvecf32;
pub mod operators_svecf32;
pub mod operators_vecf16;
pub mod operators_vecf32;
pub mod operators_veci8;
pub mod subscript_bvecf32;
pub mod subscript_svecf32;
pub mod subscript_vecf16;
pub mod subscript_vecf32;
pub mod subscript_veci8;
pub mod text_bvecf32;
pub mod text_svecf32;
pub mod text_vecf16;
pub mod text_vecf32;
pub mod text_veci8;
pub mod typmod;

use pgrx::Internal;

fn get_mut_internal<T>(internal: &mut Option<Internal>) -> Option<&mut T> {
    internal
        .as_ref()
        .and_then(|internal| unsafe { internal.get_mut::<T>() })
}
