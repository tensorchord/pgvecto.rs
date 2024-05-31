use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(Alter)]
pub fn alter(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let t = input.ident.clone();
    match input.data {
        Data::Struct(data_struct) => {
            assert!(
                data_struct.semi_token.is_none(),
                "unit struct is not supported"
            );
            let mut idents = Vec::new();
            for field in data_struct.fields {
                idents.push(field.ident.clone().expect("tuple struct is not supported"));
            }
            proc_macro::TokenStream::from(quote! {
                impl Alter for #t {
                    fn alter(&mut self, key: &[&str], value: &str) -> Result<(), AlterError> {
                        if key.is_empty() {
                            *self = toml::from_str(value).map_err(|e| AlterError::InvalidIndexOptions { reason: e.to_string() })?;
                            return Ok(());
                        }
                        match key[0] {
                            #(stringify!(#idents) => Alter::alter(&mut self.#idents, &key[1..], value),)*
                            _ => Err(AlterError::KeyNotExists { key: key.join(".") }),
                        }
                    }
                }
            })
        }
        Data::Enum(data_enum) => {
            let mut idents = Vec::new();
            let mut is_unit = false;
            let mut is_unnamed = false;
            for variant in data_enum.variants {
                idents.push(variant.ident.clone());
                match variant.fields {
                    Fields::Named(_) => panic!("named fields in enum is not supported"),
                    Fields::Unnamed(_) => is_unnamed = true,
                    Fields::Unit => is_unit = true,
                }
            }
            match (is_unit, is_unnamed) {
                (true, true) => panic!("both unit and unnamed fields in enum is not supported"),
                (false, false) => panic!("only inhabited enum is supported"),
                (true, _) => proc_macro::TokenStream::from(quote! {
                    impl Alter for #t {
                        fn alter(&mut self, key: &[&str], value: &str) -> Result<(), AlterError> {
                            if key.is_empty() {
                                *self = toml::from_str(value).map_err(|e| AlterError::InvalidIndexOptions { reason: e.to_string() })?;
                                return Ok(());
                            }
                            Err(AlterError::KeyNotExists { key: key.join(".") })
                        }
                    }
                }),
                (_, true) => proc_macro::TokenStream::from(quote! {
                    impl Alter for #t {
                        fn alter(&mut self, key: &[&str], value: &str) -> Result<(), AlterError> {
                            if key.is_empty() {
                                *self = toml::from_str(value).map_err(|e| AlterError::InvalidIndexOptions { reason: e.to_string() })?;
                                return Ok(());
                            }
                            match self {
                                #(Self::#idents(x) if stringify!(#idents).to_lowercase() == key[0] => Alter::alter(x, &key[1..], value),)*
                                _ => Err(AlterError::KeyNotExists { key: key.join(".") }),
                            }
                        }
                    }
                }),
            }
        }
        Data::Union(_) => panic!("union is not supported"),
    }
}

/// Add a wrapper function for aggregate function.
/// The wrapper function would switch memory context to aggregate context.
// pg will switch to a temporary memory context when call aggregate trans function.
// https://github.com/postgres/postgres/blob/52b49b796cc7fd976f4da6aa49c9679ecdae8bd5/src/backend/executor/nodeAgg.c#L761-L801
// If want to reuse the state in aggregate, we need to switch to aggregate context like https://github.com/postgres/postgres/blob/7c655a04a2dc84b59ed6dce97bd38b79e734ecca/src/backend/utils/adt/numeric.c#L5635-L5648.
#[proc_macro_attribute]
pub fn aggregate_func(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as syn::ItemFn);
    let fn_name = &input.sig.ident;
    let agg_wrapper_fn = syn::Ident::new(&format!("{}_agg_wrapper", fn_name), fn_name.span());
    let wrapper_fn = syn::Ident::new(&format!("{}_wrapper", fn_name), fn_name.span());
    let pg_finfo_fn = syn::Ident::new(&format!("pg_finfo_{}", agg_wrapper_fn), fn_name.span());

    let expanded = quote! {
    #input
    #[no_mangle]
    #[doc(hidden)]
    pub unsafe extern "C" fn #agg_wrapper_fn(
        _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
    ) -> ::pgrx::pg_sys::Datum {
        #[allow(unused_unsafe)]
        unsafe {
            pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(move || {
                let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
                if ::pgrx::pg_sys::AggCheckCallContext(_fcinfo, &mut agg_context) == 0 {
                    ::pgrx::error!("aggregate function called in non-aggregate context");
                }
                let old_context = ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context);
                let result = #wrapper_fn(_fcinfo);
                ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
                result
            })
        }
    }
    #[no_mangle]
    #[doc(hidden)]
    pub extern "C" fn #pg_finfo_fn(
    ) -> &'static ::pgrx::pg_sys::Pg_finfo_record {
        const V1_API: ::pgrx::pg_sys::Pg_finfo_record =
            ::pgrx::pg_sys::Pg_finfo_record { api_version: 1 };
        &V1_API
    }
    };

    proc_macro::TokenStream::from(expanded)
}
