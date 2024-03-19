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
