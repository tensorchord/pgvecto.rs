-- Drop functions that are not referenced

DROP FUNCTION _vectors_svecf32_div;
DROP FUNCTION _vectors_veci8_normalize;

-- List of shell types

CREATE TYPE sphere_vector;
CREATE TYPE sphere_vecf16;
CREATE TYPE sphere_svector;
CREATE TYPE sphere_bvector;

-- Rename internal functions

ALTER FUNCTION _vectors_vecf32_operator_minus RENAME TO _vectors_vecf32_operator_sub;
ALTER FUNCTION _vectors_vecf16_operator_minus RENAME TO _vectors_vecf16_operator_sub;
ALTER FUNCTION _vectors_svecf32_operator_minus RENAME TO _vectors_svecf32_operator_sub;

ALTER FUNCTION _vectors_vecf32_operator_cosine RENAME TO _vectors_vecf32_operator_cos;
ALTER FUNCTION _vectors_vecf16_operator_cosine RENAME TO _vectors_vecf16_operator_cos;
ALTER FUNCTION _vectors_svecf32_operator_cosine RENAME TO _vectors_svecf32_operator_cos;

ALTER FUNCTION _vectors_cast_vecf32_to_bvecf32 RENAME TO _vectors_cast_vecf32_to_bvector;
ALTER FUNCTION _vectors_cast_bvecf32_to_vecf32 RENAME TO _vectors_cast_bvector_to_vecf32;
ALTER FUNCTION _vectors_bvecf32_subscript RENAME TO _vectors_bvector_subscript;
ALTER FUNCTION _vectors_bvecf32_send RENAME TO _vectors_bvector_send;
ALTER FUNCTION _vectors_bvecf32_recv RENAME TO _vectors_bvector_recv;
ALTER FUNCTION _vectors_bvecf32_out RENAME TO _vectors_bvector_out;
ALTER FUNCTION _vectors_bvecf32_operator_xor RENAME TO _vectors_bvector_operator_xor;
ALTER FUNCTION _vectors_bvecf32_operator_or RENAME TO _vectors_bvector_operator_or;
ALTER FUNCTION _vectors_bvecf32_operator_neq RENAME TO _vectors_bvector_operator_neq;
ALTER FUNCTION _vectors_bvecf32_operator_lte RENAME TO _vectors_bvector_operator_lte;
ALTER FUNCTION _vectors_bvecf32_operator_lt RENAME TO _vectors_bvector_operator_lt;
ALTER FUNCTION _vectors_bvecf32_operator_l2 RENAME TO _vectors_bvector_operator_hamming;
ALTER FUNCTION _vectors_bvecf32_operator_jaccard RENAME TO _vectors_bvector_operator_jaccard;
ALTER FUNCTION _vectors_bvecf32_operator_gte RENAME TO _vectors_bvector_operator_gte;
ALTER FUNCTION _vectors_bvecf32_operator_gt RENAME TO _vectors_bvector_operator_gt;
ALTER FUNCTION _vectors_bvecf32_operator_eq RENAME TO _vectors_bvector_operator_eq;
ALTER FUNCTION _vectors_bvecf32_operator_dot RENAME TO _vectors_bvector_operator_dot;
ALTER FUNCTION _vectors_bvecf32_operator_and RENAME TO _vectors_bvector_operator_and;
ALTER FUNCTION _vectors_bvecf32_norm RENAME TO _vectors_bvector_norm;
ALTER FUNCTION _vectors_bvecf32_in RENAME TO _vectors_bvector_in;
ALTER FUNCTION _vectors_bvecf32_dims RENAME TO _vectors_bvector_dims;

-- List of internal functions

-- src/datatype/operators_vecf32.rs:109
-- vectors::datatype::operators_vecf32::_vectors_vecf32_sphere_l2_in
CREATE OR REPLACE FUNCTION "_vectors_vecf32_sphere_l2_in"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" sphere_vector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_sphere_l2_in_wrapper';

-- src/datatype/operators_vecf32.rs:90
-- vectors::datatype::operators_vecf32::_vectors_vecf32_sphere_dot_in
CREATE OR REPLACE FUNCTION "_vectors_vecf32_sphere_dot_in"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" sphere_vector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_sphere_dot_in_wrapper';

-- src/datatype/operators_vecf32.rs:128
-- vectors::datatype::operators_vecf32::_vectors_vecf32_sphere_cos_in
CREATE OR REPLACE FUNCTION "_vectors_vecf32_sphere_cos_in"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" sphere_vector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_sphere_cos_in_wrapper';

-- src/datatype/binary_vecf32.rs:9
-- vectors::datatype::binary_vecf32::_vectors_vecf32_send
CREATE OR REPLACE FUNCTION "_vectors_vecf32_send"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bytea /* vectors::datatype::binary::Bytea */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_send_wrapper';

-- src/datatype/binary_vecf32.rs:24
-- vectors::datatype::binary_vecf32::_vectors_vecf32_recv
CREATE OR REPLACE FUNCTION "_vectors_vecf32_recv"(
    "internal" internal, /* pgrx::datum::internal::Internal */
    "oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_recv_wrapper';

-- src/datatype/text_vecf32.rs:30
-- vectors::datatype::text_vecf32::_vectors_vecf32_out
CREATE OR REPLACE FUNCTION "_vectors_vecf32_out"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS cstring /* alloc::ffi::c_str::CString */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_out_wrapper';

-- src/datatype/operators_vecf32.rs:16
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_sub
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_sub"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_sub_wrapper';

-- src/datatype/operators_vecf32.rs:66
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_neq
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_neq"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_neq_wrapper';

-- src/datatype/operators_vecf32.rs:26
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_mul
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_mul"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_mul_wrapper';

-- src/datatype/operators_vecf32.rs:42
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_lte
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_lte"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lte_wrapper';

-- src/datatype/operators_vecf32.rs:36
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_lt
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_lt"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lt_wrapper';

-- src/datatype/operators_vecf32.rs:78
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_l2
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_l2"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_l2_wrapper';

-- src/datatype/operators_vecf32.rs:54
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_gte
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_gte"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gte_wrapper';

-- src/datatype/operators_vecf32.rs:48
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_gt
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_gt"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gt_wrapper';

-- src/datatype/operators_vecf32.rs:60
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_eq
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_eq"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_eq_wrapper';

-- src/datatype/operators_vecf32.rs:72
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_dot
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_dot"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_dot_wrapper';

-- src/datatype/operators_vecf32.rs:84
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_cos
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_cos"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_cos_wrapper';

-- src/datatype/operators_vecf32.rs:6
-- vectors::datatype::operators_vecf32::_vectors_vecf32_operator_add
CREATE OR REPLACE FUNCTION "_vectors_vecf32_operator_add"(
    "lhs" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "rhs" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_add_wrapper';

-- src/datatype/functions_vecf32.rs:15
-- vectors::datatype::functions_vecf32::_vectors_vecf32_normalize
CREATE OR REPLACE FUNCTION "_vectors_vecf32_normalize"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_normalize_wrapper';

-- src/datatype/functions_vecf32.rs:10
-- vectors::datatype::functions_vecf32::_vectors_vecf32_norm
CREATE OR REPLACE FUNCTION "_vectors_vecf32_norm"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_norm_wrapper';

-- src/datatype/text_vecf32.rs:9
-- vectors::datatype::text_vecf32::_vectors_vecf32_in
CREATE OR REPLACE FUNCTION "_vectors_vecf32_in"(
    "input" cstring, /* &core::ffi::c_str::CStr */
    "_oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_in_wrapper';

-- src/datatype/functions_vecf32.rs:5
-- vectors::datatype::functions_vecf32::_vectors_vecf32_dims
CREATE OR REPLACE FUNCTION "_vectors_vecf32_dims"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_dims_wrapper';

-- src/datatype/aggregate_vecf32.rs:142
-- vectors::datatype::aggregate_vecf32::_vectors_vecf32_aggregate_sum_finalfunc
CREATE OR REPLACE FUNCTION "_vectors_vecf32_aggregate_sum_finalfunc"(
    "state" internal /* pgrx::datum::internal::Internal */
) RETURNS vector /* core::option::Option<vectors::datatype::memory_vecf32::Vecf32Output> */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_aggregate_sum_finalfunc_wrapper';

-- src/datatype/aggregate_vecf32.rs:124
-- vectors::datatype::aggregate_vecf32::_vectors_vecf32_aggregate_avg_finalfunc
CREATE OR REPLACE FUNCTION "_vectors_vecf32_aggregate_avg_finalfunc"(
    "state" internal /* pgrx::datum::internal::Internal */
) RETURNS vector /* core::option::Option<vectors::datatype::memory_vecf32::Vecf32Output> */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf32_aggregate_avg_finalfunc_wrapper';

-- src/datatype/operators_vecf16.rs:109
-- vectors::datatype::operators_vecf16::_vectors_vecf16_sphere_l2_in
CREATE OR REPLACE FUNCTION "_vectors_vecf16_sphere_l2_in"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" sphere_vecf16 /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_sphere_l2_in_wrapper';

-- src/datatype/operators_vecf16.rs:90
-- vectors::datatype::operators_vecf16::_vectors_vecf16_sphere_dot_in
CREATE OR REPLACE FUNCTION "_vectors_vecf16_sphere_dot_in"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" sphere_vecf16 /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_sphere_dot_in_wrapper';

-- src/datatype/operators_vecf16.rs:128
-- vectors::datatype::operators_vecf16::_vectors_vecf16_sphere_cos_in
CREATE OR REPLACE FUNCTION "_vectors_vecf16_sphere_cos_in"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" sphere_vecf16 /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_sphere_cos_in_wrapper';

-- src/datatype/binary_vecf16.rs:10
-- vectors::datatype::binary_vecf16::_vectors_vecf16_send
CREATE OR REPLACE FUNCTION "_vectors_vecf16_send"(
    "vector" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bytea /* vectors::datatype::binary::Bytea */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_send_wrapper';

-- src/datatype/binary_vecf16.rs:25
-- vectors::datatype::binary_vecf16::_vectors_vecf16_recv
CREATE OR REPLACE FUNCTION "_vectors_vecf16_recv"(
    "internal" internal, /* pgrx::datum::internal::Internal */
    "oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_recv_wrapper';

-- src/datatype/text_vecf16.rs:30
-- vectors::datatype::text_vecf16::_vectors_vecf16_out
CREATE OR REPLACE FUNCTION "_vectors_vecf16_out"(
    "vector" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS cstring /* alloc::ffi::c_str::CString */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_out_wrapper';

-- src/datatype/operators_vecf16.rs:16
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_sub
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_sub"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_sub_wrapper';

-- src/datatype/operators_vecf16.rs:66
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_neq
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_neq"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_neq_wrapper';

-- src/datatype/operators_vecf16.rs:26
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_mul
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_mul"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_mul_wrapper';

-- src/datatype/operators_vecf16.rs:42
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_lte
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_lte"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lte_wrapper';

-- src/datatype/operators_vecf16.rs:36
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_lt
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_lt"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lt_wrapper';

-- src/datatype/operators_vecf16.rs:78
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_l2
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_l2"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_l2_wrapper';

-- src/datatype/operators_vecf16.rs:54
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_gte
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_gte"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gte_wrapper';

-- src/datatype/operators_vecf16.rs:48
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_gt
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_gt"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gt_wrapper';

-- src/datatype/operators_vecf16.rs:60
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_eq
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_eq"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_eq_wrapper';

-- src/datatype/operators_vecf16.rs:72
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_dot
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_dot"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_dot_wrapper';

-- src/datatype/operators_vecf16.rs:84
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_cos
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_cos"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_cos_wrapper';

-- src/datatype/operators_vecf16.rs:6
-- vectors::datatype::operators_vecf16::_vectors_vecf16_operator_add
CREATE OR REPLACE FUNCTION "_vectors_vecf16_operator_add"(
    "lhs" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "rhs" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_add_wrapper';

-- src/datatype/functions_vecf16.rs:15
-- vectors::datatype::functions_vecf16::_vectors_vecf16_normalize
CREATE OR REPLACE FUNCTION "_vectors_vecf16_normalize"(
    "vector" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_normalize_wrapper';

-- src/datatype/functions_vecf16.rs:10
-- vectors::datatype::functions_vecf16::_vectors_vecf16_norm
CREATE OR REPLACE FUNCTION "_vectors_vecf16_norm"(
    "vector" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_norm_wrapper';

-- src/datatype/text_vecf16.rs:9
-- vectors::datatype::text_vecf16::_vectors_vecf16_in
CREATE OR REPLACE FUNCTION "_vectors_vecf16_in"(
    "input" cstring, /* &core::ffi::c_str::CStr */
    "_oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_in_wrapper';

-- src/datatype/functions_vecf16.rs:5
-- vectors::datatype::functions_vecf16::_vectors_vecf16_dims
CREATE OR REPLACE FUNCTION "_vectors_vecf16_dims"(
    "vector" vecf16 /* vectors::datatype::memory_vecf16::Vecf16Input */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_vecf16_dims_wrapper';

-- src/datatype/typmod.rs:82
-- vectors::datatype::typmod::_vectors_typmod_out
CREATE OR REPLACE FUNCTION "_vectors_typmod_out"(
    "typmod" INT /* i32 */
) RETURNS cstring /* alloc::ffi::c_str::CString */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_typmod_out_wrapper';

-- src/datatype/typmod.rs:46
-- vectors::datatype::typmod::_vectors_typmod_in_65535
CREATE OR REPLACE FUNCTION "_vectors_typmod_in_65535"(
    "list" cstring[] /* pgrx::datum::array::Array<&core::ffi::c_str::CStr> */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_typmod_in_65535_wrapper';

-- src/datatype/typmod.rs:64
-- vectors::datatype::typmod::_vectors_typmod_in_1048575
CREATE OR REPLACE FUNCTION "_vectors_typmod_in_1048575"(
    "list" cstring[] /* pgrx::datum::array::Array<&core::ffi::c_str::CStr> */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_typmod_in_1048575_wrapper';

-- src/datatype/functions_svecf32.rs:21
-- vectors::datatype::functions_svecf32::_vectors_to_svector
CREATE OR REPLACE FUNCTION "_vectors_to_svector"(
    "dims" INT, /* i32 */
    "index" INT[], /* pgrx::datum::array::Array<i32> */
    "value" real[] /* pgrx::datum::array::Array<f32> */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_to_svector_wrapper';

-- src/embedding/mod.rs:7
-- vectors::embedding::_vectors_text2vec_openai
CREATE OR REPLACE FUNCTION "_vectors_text2vec_openai"(
    "input" TEXT, /* alloc::string::String */
    "model" TEXT /* alloc::string::String */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
STRICT VOLATILE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_text2vec_openai_wrapper';

-- src/datatype/operators_svecf32.rs:109
-- vectors::datatype::operators_svecf32::_vectors_svecf32_sphere_l2_in
CREATE OR REPLACE FUNCTION "_vectors_svecf32_sphere_l2_in"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" sphere_svector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_sphere_l2_in_wrapper';

-- src/datatype/operators_svecf32.rs:90
-- vectors::datatype::operators_svecf32::_vectors_svecf32_sphere_dot_in
CREATE OR REPLACE FUNCTION "_vectors_svecf32_sphere_dot_in"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" sphere_svector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_sphere_dot_in_wrapper';

-- src/datatype/operators_svecf32.rs:128
-- vectors::datatype::operators_svecf32::_vectors_svecf32_sphere_cos_in
CREATE OR REPLACE FUNCTION "_vectors_svecf32_sphere_cos_in"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" sphere_svector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_sphere_cos_in_wrapper';

-- src/datatype/binary_svecf32.rs:10
-- vectors::datatype::binary_svecf32::_vectors_svecf32_send
CREATE OR REPLACE FUNCTION "_vectors_svecf32_send"(
    "vector" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bytea /* vectors::datatype::binary::Bytea */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_send_wrapper';

-- src/datatype/binary_svecf32.rs:29
-- vectors::datatype::binary_svecf32::_vectors_svecf32_recv
CREATE OR REPLACE FUNCTION "_vectors_svecf32_recv"(
    "internal" internal, /* pgrx::datum::internal::Internal */
    "oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_recv_wrapper';

-- src/datatype/text_svecf32.rs:77
-- vectors::datatype::text_svecf32::_vectors_svecf32_out
CREATE OR REPLACE FUNCTION "_vectors_svecf32_out"(
    "vector" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS cstring /* alloc::ffi::c_str::CString */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_out_wrapper';

-- src/datatype/operators_svecf32.rs:16
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_sub
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_sub"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_sub_wrapper';

-- src/datatype/operators_svecf32.rs:66
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_neq
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_neq"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_neq_wrapper';

-- src/datatype/operators_svecf32.rs:26
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_mul
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_mul"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_mul_wrapper';

-- src/datatype/operators_svecf32.rs:42
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_lte
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_lte"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_lte_wrapper';

-- src/datatype/operators_svecf32.rs:36
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_lt
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_lt"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_lt_wrapper';

-- src/datatype/operators_svecf32.rs:78
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_l2
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_l2"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_l2_wrapper';

-- src/datatype/operators_svecf32.rs:54
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_gte
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_gte"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_gte_wrapper';

-- src/datatype/operators_svecf32.rs:48
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_gt
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_gt"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_gt_wrapper';

-- src/datatype/operators_svecf32.rs:60
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_eq
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_eq"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_eq_wrapper';

-- src/datatype/operators_svecf32.rs:72
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_dot
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_dot"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_dot_wrapper';

-- src/datatype/operators_svecf32.rs:84
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_cos
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_cos"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_cos_wrapper';

-- src/datatype/operators_svecf32.rs:6
-- vectors::datatype::operators_svecf32::_vectors_svecf32_operator_add
CREATE OR REPLACE FUNCTION "_vectors_svecf32_operator_add"(
    "lhs" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "rhs" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_add_wrapper';

-- src/datatype/functions_svecf32.rs:16
-- vectors::datatype::functions_svecf32::_vectors_svecf32_normalize
CREATE OR REPLACE FUNCTION "_vectors_svecf32_normalize"(
    "vector" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_normalize_wrapper';

-- src/datatype/functions_svecf32.rs:11
-- vectors::datatype::functions_svecf32::_vectors_svecf32_norm
CREATE OR REPLACE FUNCTION "_vectors_svecf32_norm"(
    "vector" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_norm_wrapper';

-- There might be a conflict of `typmod` or `_typmod`
-- If installed directly from 0.3.0, a break happens
-- If updated from 0.2.1 to 0.3.0, update can be continue
DO $$
DECLARE
    func_svecf32_arg_3 TEXT;
BEGIN
    SELECT parameter_name INTO func_svecf32_arg_3
    FROM information_schema.routines
        LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
    WHERE routines.specific_schema='vectors' AND routines.routine_name='_vectors_svecf32_in' AND parameters.ordinal_position=3 
    ORDER BY routines.routine_name, parameters.ordinal_position;
    IF func_svecf32_arg_3 = '_typmod' THEN
        -- src/datatype/text_svecf32.rs:10
        -- vectors::datatype::text_svecf32::_vectors_svecf32_in
        CREATE OR REPLACE FUNCTION "_vectors_svecf32_in"(
            "input" cstring, /* &core::ffi::c_str::CStr */
            "_oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
            "_typmod" INT /* i32 */
        ) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
        IMMUTABLE STRICT PARALLEL SAFE
        LANGUAGE c /* Rust */
        AS 'MODULE_PATHNAME', '_vectors_svecf32_in_wrapper';
    ELSE
        -- src/datatype/text_svecf32.rs:10
        -- vectors::datatype::text_svecf32::_vectors_svecf32_in
        CREATE OR REPLACE FUNCTION "_vectors_svecf32_in"(
            "input" cstring, /* &core::ffi::c_str::CStr */
            "_oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
            "typmod" INT /* i32 */
        ) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
        IMMUTABLE STRICT PARALLEL SAFE
        LANGUAGE c /* Rust */
        AS 'MODULE_PATHNAME', '_vectors_svecf32_in_wrapper';
    END IF;
END $$;

-- src/datatype/functions_svecf32.rs:6
-- vectors::datatype::functions_svecf32::_vectors_svecf32_dims
CREATE OR REPLACE FUNCTION "_vectors_svecf32_dims"(
    "vector" svector /* vectors::datatype::memory_svecf32::SVecf32Input */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_dims_wrapper';

-- src/datatype/aggregate_svecf32.rs:276
-- vectors::datatype::aggregate_svecf32::_vectors_svecf32_aggregate_sum_finalfunc
CREATE OR REPLACE FUNCTION "_vectors_svecf32_aggregate_sum_finalfunc"(
    "state" internal /* pgrx::datum::internal::Internal */
) RETURNS svector /* core::option::Option<vectors::datatype::memory_svecf32::SVecf32Output> */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_aggregate_sum_finalfunc_wrapper';

-- src/datatype/aggregate_svecf32.rs:253
-- vectors::datatype::aggregate_svecf32::_vectors_svecf32_aggregate_avg_finalfunc
CREATE OR REPLACE FUNCTION "_vectors_svecf32_aggregate_avg_finalfunc"(
    "state" internal /* pgrx::datum::internal::Internal */
) RETURNS svector /* core::option::Option<vectors::datatype::memory_svecf32::SVecf32Output> */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_svecf32_aggregate_avg_finalfunc_wrapper';

-- src/index/functions.rs:7
-- vectors::index::functions::_vectors_pgvectors_upgrade
CREATE OR REPLACE FUNCTION "_vectors_pgvectors_upgrade"() RETURNS void
STRICT VOLATILE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

-- src/index/views.rs:17
-- vectors::index::views::_vectors_index_stat
CREATE OR REPLACE FUNCTION "_vectors_index_stat"(
    "oid" oid /* pgrx_pg_sys::submodules::oids::Oid */
) RETURNS vector_index_stat /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
STRICT VOLATILE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_index_stat_wrapper';

-- src/index/functions.rs:15
-- vectors::index::functions::_vectors_fence_vector_index
CREATE OR REPLACE FUNCTION "_vectors_fence_vector_index"(
    "oid" oid /* pgrx_pg_sys::submodules::oids::Oid */
) RETURNS void
STRICT VOLATILE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_fence_vector_index_wrapper';

-- src/datatype/casts.rs:35
-- vectors::datatype::casts::_vectors_cast_vecf32_to_vecf16
CREATE OR REPLACE FUNCTION "_vectors_cast_vecf32_to_vecf16"(
    "vector" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS vecf16 /* vectors::datatype::memory_vecf16::Vecf16Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_vecf16_wrapper';

-- src/datatype/casts.rs:55
-- vectors::datatype::casts::_vectors_cast_vecf32_to_svecf32
CREATE OR REPLACE FUNCTION "_vectors_cast_vecf32_to_svecf32"(
    "vector" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS svector /* vectors::datatype::memory_svecf32::SVecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_svecf32_wrapper';

-- src/datatype/casts.rs:89
-- vectors::datatype::casts::_vectors_cast_vecf32_to_bvector
CREATE OR REPLACE FUNCTION "_vectors_cast_vecf32_to_bvector"(
    "vector" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_bvector_wrapper';

-- src/datatype/casts.rs:26
-- vectors::datatype::casts::_vectors_cast_vecf32_to_array
CREATE OR REPLACE FUNCTION "_vectors_cast_vecf32_to_array"(
    "vector" vector, /* vectors::datatype::memory_vecf32::Vecf32Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS real[] /* alloc::vec::Vec<f32> */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_array_wrapper';

-- src/datatype/casts.rs:45
-- vectors::datatype::casts::_vectors_cast_vecf16_to_vecf32
CREATE OR REPLACE FUNCTION "_vectors_cast_vecf16_to_vecf32"(
    "vector" vecf16, /* vectors::datatype::memory_vecf16::Vecf16Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_vecf16_to_vecf32_wrapper';

-- src/datatype/casts.rs:73
-- vectors::datatype::casts::_vectors_cast_svecf32_to_vecf32
CREATE OR REPLACE FUNCTION "_vectors_cast_svecf32_to_vecf32"(
    "vector" svector, /* vectors::datatype::memory_svecf32::SVecf32Input */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_svecf32_to_vecf32_wrapper';

-- src/datatype/casts.rs:108
-- vectors::datatype::casts::_vectors_cast_bvector_to_vecf32
CREATE OR REPLACE FUNCTION "_vectors_cast_bvector_to_vecf32"(
    "vector" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "_typmod" INT, /* i32 */
    "_explicit" bool /* bool */
) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_cast_bvector_to_vecf32_wrapper';

-- There might be a conflict of `typmod` or `_typmod`
DO $$
DECLARE
    func_arg_2 TEXT;
BEGIN
    SELECT parameter_name INTO func_arg_2
    FROM information_schema.routines
        LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
    WHERE routines.specific_schema='vectors' AND routines.routine_name='_vectors_cast_array_to_vecf32' AND parameters.ordinal_position=2 
    ORDER BY routines.routine_name, parameters.ordinal_position;
    IF func_arg_2 = '_typmod' THEN
        -- src/datatype/casts.rs:10
        -- vectors::datatype::casts::_vectors_cast_array_to_vecf32
        CREATE OR REPLACE FUNCTION "_vectors_cast_array_to_vecf32"(
            "array" real[], /* pgrx::datum::array::Array<f32> */
            "_typmod" INT, /* i32 */
            "_explicit" bool /* bool */
        ) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
        IMMUTABLE STRICT PARALLEL SAFE
        LANGUAGE c /* Rust */
        AS 'MODULE_PATHNAME', '_vectors_cast_array_to_vecf32_wrapper';
    ELSE
        -- src/datatype/casts.rs:10
        -- vectors::datatype::casts::_vectors_cast_array_to_vecf32
        CREATE OR REPLACE FUNCTION "_vectors_cast_array_to_vecf32"(
            "array" real[], /* pgrx::datum::array::Array<f32> */
            "typmod" INT, /* i32 */
            "_explicit" bool /* bool */
        ) RETURNS vector /* vectors::datatype::memory_vecf32::Vecf32Output */
        IMMUTABLE STRICT PARALLEL SAFE
        LANGUAGE c /* Rust */
        AS 'MODULE_PATHNAME', '_vectors_cast_array_to_vecf32_wrapper';
    END IF;
END $$;

-- src/datatype/subscript_bvector.rs:10
-- vectors::datatype::subscript_bvector::_vectors_bvector_subscript
CREATE OR REPLACE FUNCTION _vectors_bvector_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvector_subscript_wrapper';

-- src/datatype/operators_bvector.rs:128
-- vectors::datatype::operators_bvector::_vectors_bvector_sphere_jaccard_in
CREATE OR REPLACE FUNCTION "_vectors_bvector_sphere_jaccard_in"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" sphere_bvector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_sphere_jaccard_in_wrapper';

-- src/datatype/operators_bvector.rs:109
-- vectors::datatype::operators_bvector::_vectors_bvector_sphere_hamming_in
CREATE OR REPLACE FUNCTION "_vectors_bvector_sphere_hamming_in"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" sphere_bvector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_sphere_hamming_in_wrapper';

-- src/datatype/operators_bvector.rs:90
-- vectors::datatype::operators_bvector::_vectors_bvector_sphere_dot_in
CREATE OR REPLACE FUNCTION "_vectors_bvector_sphere_dot_in"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" sphere_bvector /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_sphere_dot_in_wrapper';

-- src/datatype/binary_bvector.rs:10
-- vectors::datatype::binary_bvector::_vectors_bvector_send
CREATE OR REPLACE FUNCTION "_vectors_bvector_send"(
    "vector" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bytea /* vectors::datatype::binary::Bytea */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_send_wrapper';

-- src/datatype/binary_bvector.rs:25
-- vectors::datatype::binary_bvector::_vectors_bvector_recv
CREATE OR REPLACE FUNCTION "_vectors_bvector_recv"(
    "internal" internal, /* pgrx::datum::internal::Internal */
    "oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_recv_wrapper';

-- src/datatype/text_bvector.rs:42
-- vectors::datatype::text_bvector::_vectors_bvector_out
CREATE OR REPLACE FUNCTION "_vectors_bvector_out"(
    "vector" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS cstring /* alloc::ffi::c_str::CString */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_out_wrapper';

-- src/datatype/operators_bvector.rs:26
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_xor
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_xor"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_xor_wrapper';

-- src/datatype/operators_bvector.rs:16
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_or
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_or"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_or_wrapper';

-- src/datatype/operators_bvector.rs:66
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_neq
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_neq"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_neq_wrapper';

-- src/datatype/operators_bvector.rs:42
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_lte
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_lte"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_lte_wrapper';

-- src/datatype/operators_bvector.rs:36
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_lt
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_lt"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_lt_wrapper';

-- src/datatype/operators_bvector.rs:84
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_jaccard
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_jaccard"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_jaccard_wrapper';

-- src/datatype/operators_bvector.rs:78
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_hamming
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_hamming"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_hamming_wrapper';

-- src/datatype/operators_bvector.rs:54
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_gte
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_gte"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_gte_wrapper';

-- src/datatype/operators_bvector.rs:48
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_gt
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_gt"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_gt_wrapper';

-- src/datatype/operators_bvector.rs:60
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_eq
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_eq"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_eq_wrapper';

-- src/datatype/operators_bvector.rs:72
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_dot
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_dot"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_dot_wrapper';

-- src/datatype/operators_bvector.rs:6
-- vectors::datatype::operators_bvector::_vectors_bvector_operator_and
CREATE OR REPLACE FUNCTION "_vectors_bvector_operator_and"(
    "lhs" bvector, /* vectors::datatype::memory_bvector::BVectorInput */
    "rhs" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_operator_and_wrapper';

-- src/datatype/functions_bvector.rs:11
-- vectors::datatype::functions_bvector::_vectors_bvector_norm
CREATE OR REPLACE FUNCTION "_vectors_bvector_norm"(
    "vector" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS real /* f32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_norm_wrapper';

-- src/datatype/text_bvector.rs:9
-- vectors::datatype::text_bvector::_vectors_bvector_in
CREATE OR REPLACE FUNCTION "_vectors_bvector_in"(
    "input" cstring, /* &core::ffi::c_str::CStr */
    "_oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "typmod" INT /* i32 */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_in_wrapper';

-- src/datatype/functions_bvector.rs:6
-- vectors::datatype::functions_bvector::_vectors_bvector_dims
CREATE OR REPLACE FUNCTION "_vectors_bvector_dims"(
    "vector" bvector /* vectors::datatype::memory_bvector::BVectorInput */
) RETURNS INT /* i32 */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_bvector_dims_wrapper';

-- src/datatype/functions_bvector.rs:16
-- vectors::datatype::functions_bvector::_vectors_binarize
CREATE OR REPLACE FUNCTION "_vectors_binarize"(
    "vector" vector /* vectors::datatype::memory_vecf32::Vecf32Input */
) RETURNS bvector /* vectors::datatype::memory_bvector::BVectorOutput */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_binarize_wrapper';

-- src/index/views.rs:7
-- vectors::index::views::_vectors_alter_vector_index
CREATE OR REPLACE FUNCTION "_vectors_alter_vector_index"(
    "oid" oid, /* pgrx_pg_sys::submodules::oids::Oid */
    "key" TEXT, /* alloc::string::String */
    "value" TEXT /* alloc::string::String */
) RETURNS void
STRICT VOLATILE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', '_vectors_alter_vector_index_wrapper';

-- Drop operator classes

DROP OPERATOR CLASS vector_l2_ops USING vectors;
DROP OPERATOR CLASS vector_dot_ops USING vectors;
DROP OPERATOR CLASS vector_cos_ops USING vectors;
DROP OPERATOR CLASS vecf16_l2_ops USING vectors;
DROP OPERATOR CLASS vecf16_dot_ops USING vectors;
DROP OPERATOR CLASS vecf16_cos_ops USING vectors;
DROP OPERATOR CLASS svector_l2_ops USING vectors;
DROP OPERATOR CLASS svector_dot_ops USING vectors;
DROP OPERATOR CLASS svector_cos_ops USING vectors;
DROP OPERATOR CLASS bvector_l2_ops USING vectors;
DROP OPERATOR CLASS bvector_dot_ops USING vectors;
DROP OPERATOR CLASS bvector_cos_ops USING vectors;
DROP OPERATOR CLASS bvector_jaccard_ops USING vectors;
DROP OPERATOR CLASS veci8_l2_ops USING vectors;
DROP OPERATOR CLASS veci8_dot_ops USING vectors;
DROP OPERATOR CLASS veci8_cos_ops USING vectors;

-- Drop operator families

DROP OPERATOR FAMILY vector_l2_ops USING vectors;
DROP OPERATOR FAMILY vector_dot_ops USING vectors;
DROP OPERATOR FAMILY vector_cos_ops USING vectors;
DROP OPERATOR FAMILY vecf16_l2_ops USING vectors;
DROP OPERATOR FAMILY vecf16_dot_ops USING vectors;
DROP OPERATOR FAMILY vecf16_cos_ops USING vectors;
DROP OPERATOR FAMILY svector_l2_ops USING vectors;
DROP OPERATOR FAMILY svector_dot_ops USING vectors;
DROP OPERATOR FAMILY svector_cos_ops USING vectors;
DROP OPERATOR FAMILY bvector_l2_ops USING vectors;
DROP OPERATOR FAMILY bvector_dot_ops USING vectors;
DROP OPERATOR FAMILY bvector_cos_ops USING vectors;
DROP OPERATOR FAMILY bvector_jaccard_ops USING vectors;
DROP OPERATOR FAMILY veci8_l2_ops USING vectors;
DROP OPERATOR FAMILY veci8_dot_ops USING vectors;
DROP OPERATOR FAMILY veci8_cos_ops USING vectors;

-- Drop casts

DROP CAST (veci8 AS vector);
DROP FUNCTION _vectors_cast_veci8_to_vecf32(veci8, integer, boolean);
DROP CAST (vector AS veci8);
DROP FUNCTION _vectors_cast_vecf32_to_veci8(vector, integer, boolean);

-- Drop functions

DROP FUNCTION vector_dims(veci8);
DROP FUNCTION _vectors_veci8_dims(veci8);
DROP FUNCTION vector_norm(veci8);
DROP FUNCTION _vectors_veci8_norm(veci8);
DROP FUNCTION to_veci8(int, real, real, int[]);
DROP FUNCTION _vectors_to_veci8(int, real, real, int[]);

-- Drop operators

DROP OPERATOR +(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_add;
DROP OPERATOR -(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_minus;
DROP OPERATOR *(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_mul;
DROP OPERATOR =(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_eq;
DROP OPERATOR <>(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_neq;
DROP OPERATOR <(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_lt;
DROP OPERATOR >(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_gt;
DROP OPERATOR <=(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_lte;
DROP OPERATOR >=(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_gte;
DROP OPERATOR <->(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_l2;
DROP OPERATOR <#>(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_dot;
DROP OPERATOR <=>(veci8,veci8);
DROP FUNCTION _vectors_veci8_operator_cosine;
DROP OPERATOR <=>(bvector,bvector);
DROP FUNCTION _vectors_bvecf32_operator_cosine;

-- Drop types

DO $$
DECLARE
    depcount_veci8 INT;
    depcount_in INT;
    depcount_out INT;
    depcount_recv INT;
    depcount_send INT;
BEGIN
    SELECT COUNT(*) INTO depcount_veci8 FROM pg_depend d WHERE d.refobjid = 'vectors.veci8'::regtype;
    SELECT COUNT(*) INTO depcount_in FROM pg_depend d WHERE d.refobjid = 'vectors._vectors_veci8_in(cstring,oid,integer)'::regprocedure;
    SELECT COUNT(*) INTO depcount_out FROM pg_depend d WHERE d.refobjid = 'vectors._vectors_veci8_out(vectors.veci8)'::regprocedure;
    SELECT COUNT(*) INTO depcount_recv FROM pg_depend d WHERE d.refobjid = 'vectors._vectors_veci8_recv(internal,oid,integer)'::regprocedure;
    SELECT COUNT(*) INTO depcount_send FROM pg_depend d WHERE d.refobjid = 'vectors._vectors_veci8_send(vectors.veci8)'::regprocedure;
    IF depcount_veci8 <> 5 OR depcount_in <> 1 OR depcount_out <> 1 OR depcount_recv <> 1 OR depcount_send <> 1 THEN
        RAISE EXCEPTION 'Update fails because you still need type `veci8`.';
    END IF;
END $$;

DROP TYPE veci8 CASCADE;
-- DROP FUNCTION _vectors_veci8_in;
-- DROP FUNCTION _vectors_veci8_out;
-- DROP FUNCTION _vectors_veci8_recv;
-- DROP FUNCTION _vectors_veci8_send;
DROP FUNCTION _vectors_veci8_subscript;

-- List of data types

CREATE TYPE sphere_vector AS (
    center vector,
    radius REAL
);

CREATE TYPE sphere_vecf16 AS (
    center vecf16,
    radius REAL
);

CREATE TYPE sphere_svector AS (
    center svector,
    radius REAL
);

CREATE TYPE sphere_bvector AS (
    center bvector,
    radius REAL
);

-- List of operators

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_vecf32_sphere_l2_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_vecf16_sphere_l2_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_svecf32_sphere_l2_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_bvector_sphere_hamming_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_vecf32_sphere_dot_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_vecf16_sphere_dot_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_svecf32_sphere_dot_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_bvector_sphere_dot_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_vecf32_sphere_cos_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_vecf16_sphere_cos_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_svecf32_sphere_cos_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<~>> (
    PROCEDURE = _vectors_bvector_sphere_jaccard_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<~>>
);

-- List of functions

CREATE OR REPLACE FUNCTION alter_vector_index("index" OID, "key" TEXT, "value" TEXT) RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_alter_vector_index_wrapper';

CREATE FUNCTION fence_vector_index(oid) RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_fence_vector_index_wrapper';

CREATE OR REPLACE FUNCTION vector_dims(vector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_dims_wrapper';

CREATE OR REPLACE FUNCTION vector_dims(vecf16) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_dims_wrapper';

CREATE OR REPLACE FUNCTION vector_dims(svector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_dims_wrapper';

CREATE OR REPLACE FUNCTION vector_dims(bvector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvector_dims_wrapper';

CREATE OR REPLACE FUNCTION vector_norm(vector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_norm_wrapper';

CREATE OR REPLACE FUNCTION vector_norm(vecf16) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_norm_wrapper';

CREATE OR REPLACE FUNCTION vector_norm(svector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_norm_wrapper';

CREATE OR REPLACE FUNCTION vector_norm(bvector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvector_norm_wrapper';

CREATE FUNCTION vector_normalize(vector) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_normalize_wrapper';

CREATE FUNCTION vector_normalize(vecf16) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_normalize_wrapper';

CREATE FUNCTION vector_normalize(svector) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_normalize_wrapper';

CREATE FUNCTION sphere(vector, real) RETURNS sphere_vector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(vecf16, real) RETURNS sphere_vecf16
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(svector, real) RETURNS sphere_svector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(bvector, real) RETURNS sphere_bvector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

-- List of operator families

CREATE OPERATOR FAMILY vector_l2_ops USING vectors;

CREATE OPERATOR FAMILY vector_dot_ops USING vectors;

CREATE OPERATOR FAMILY vector_cos_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_l2_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_dot_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_cos_ops USING vectors;

CREATE OPERATOR FAMILY svector_l2_ops USING vectors;

CREATE OPERATOR FAMILY svector_dot_ops USING vectors;

CREATE OPERATOR FAMILY svector_cos_ops USING vectors;

CREATE OPERATOR FAMILY bvector_hamming_ops USING vectors;

CREATE OPERATOR FAMILY bvector_dot_ops USING vectors;

CREATE OPERATOR FAMILY bvector_jaccard_ops USING vectors;

-- List of operator classes

CREATE OPERATOR CLASS vector_l2_ops
    FOR TYPE vector USING vectors FAMILY vector_l2_ops AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vector_dot_ops
    FOR TYPE vector USING vectors FAMILY vector_dot_ops AS
    OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vector_cos_ops
    FOR TYPE vector USING vectors FAMILY vector_cos_ops AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_l2_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_l2_ops AS
    OPERATOR 1 <-> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_dot_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_dot_ops AS
    OPERATOR 1 <#> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_cos_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_cos_ops AS
    OPERATOR 1 <=> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS svector_l2_ops
    FOR TYPE svector USING vectors FAMILY svector_l2_ops AS
    OPERATOR 1 <-> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS svector_dot_ops
    FOR TYPE svector USING vectors FAMILY svector_dot_ops AS
    OPERATOR 1 <#> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS svector_cos_ops
    FOR TYPE svector USING vectors FAMILY svector_cos_ops AS
    OPERATOR 1 <=> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_hamming_ops
    FOR TYPE bvector USING vectors FAMILY bvector_hamming_ops AS
    OPERATOR 1 <-> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (bvector, sphere_bvector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_dot_ops
    FOR TYPE bvector USING vectors FAMILY bvector_dot_ops AS
    OPERATOR 1 <#> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (bvector, sphere_bvector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_jaccard_ops
    FOR TYPE bvector USING vectors FAMILY bvector_jaccard_ops AS
    OPERATOR 1 <~> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<~>> (bvector, sphere_bvector) FOR SEARCH;
