// Referenced symbols must exist in the dynamic library when dropping functions.
// So we should never remove symbols used by schema, otherwise there will be errors in upgrade.
// Reference:
// * https://www.postgresql.org/message-id/CACX+KaPOzzRHEt4w_=iqKbTpMKjyrUGVng1C749yP3r6dprtcg@mail.gmail.com
// * https://github.com/tensorchord/pgvecto.rs/issues/397

macro_rules! symbol {
    ($t:ident) => {
        paste::paste! {
            #[no_mangle]
            #[doc(hidden)]
            #[pgrx::pg_guard]
            extern "C" fn [<$t _wrapper>](_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> pgrx::pg_sys::Datum {
                pgrx::error!(
                    "the symbol {} is removed in the extension; please run extension update scripts",
                    stringify!($t),
                );
            }
            #[no_mangle]
            #[doc(hidden)]
            pub extern "C" fn [<pg_finfo_ $t _wrapper>]() -> &'static ::pgrx::pg_sys::Pg_finfo_record {
                const V1_API: ::pgrx::pg_sys::Pg_finfo_record = ::pgrx::pg_sys::Pg_finfo_record {
                    api_version: 1,
                };
                &V1_API
            }
        }
    };
}

// 0.2.1--0.3.0
symbol!(_vectors_ai_embedding_vector);
symbol!(_vectors_typmod_in);

// 0.3.0--0.4.0
symbol!(_vectors_veci8_subscript);
symbol!(_vectors_veci8_send);
symbol!(_vectors_veci8_recv);
symbol!(_vectors_veci8_out);
symbol!(_vectors_veci8_operator_neq);
symbol!(_vectors_veci8_operator_mul);
symbol!(_vectors_veci8_operator_minus);
symbol!(_vectors_veci8_operator_lte);
symbol!(_vectors_veci8_operator_lt);
symbol!(_vectors_veci8_operator_l2);
symbol!(_vectors_veci8_operator_gte);
symbol!(_vectors_veci8_operator_gt);
symbol!(_vectors_veci8_operator_eq);
symbol!(_vectors_veci8_operator_dot);
symbol!(_vectors_veci8_operator_cosine);
symbol!(_vectors_veci8_operator_add);
symbol!(_vectors_veci8_normalize);
symbol!(_vectors_veci8_norm);
symbol!(_vectors_veci8_in);
symbol!(_vectors_veci8_dims);
symbol!(_vectors_to_veci8);
symbol!(_vectors_cast_veci8_to_vecf32);
symbol!(_vectors_cast_vecf32_to_veci8);
