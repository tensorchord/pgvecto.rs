mod target;

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

struct MultiversionVersion {
    target: String,
    import: bool,
}

impl syn::parse::Parse for MultiversionVersion {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let lookahead1 = input.lookahead1();
        if lookahead1.peek(syn::Token![@]) {
            let _: syn::Token![@] = input.parse()?;
            let target: syn::LitStr = input.parse()?;
            Ok(Self {
                target: target.value(),
                import: true,
            })
        } else {
            let target: syn::LitStr = input.parse()?;
            Ok(Self {
                target: target.value(),
                import: false,
            })
        }
    }
}

struct Multiversion {
    versions: syn::punctuated::Punctuated<MultiversionVersion, syn::Token![,]>,
}

impl syn::parse::Parse for Multiversion {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Multiversion {
            versions: syn::punctuated::Punctuated::parse_terminated(input)?,
        })
    }
}

#[proc_macro_attribute]
pub fn multiversion(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let attr = syn::parse_macro_input!(attr as Multiversion);
    let item_fn = syn::parse::<syn::ItemFn>(item).expect("not a function item");
    let syn::ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = item_fn;
    let name = sig.ident.to_string();
    if sig.constness.is_some() {
        panic!("const functions are not supported");
    }
    if sig.asyncness.is_some() {
        panic!("async functions are not supported");
    }
    let generics_params = sig.generics.params.clone();
    for generic_param in generics_params.iter() {
        if !matches!(generic_param, syn::GenericParam::Lifetime(_)) {
            panic!("generic parameters are not supported");
        }
    }
    let generics_where = sig.generics.where_clause.clone();
    let inputs = sig.inputs.clone();
    let arguments = {
        let mut list = vec![];
        for x in sig.inputs.iter() {
            if let syn::FnArg::Typed(y) = x {
                if let syn::Pat::Ident(ident) = *y.pat.clone() {
                    list.push(ident);
                } else {
                    panic!("patterns on parameters are not supported")
                }
            } else {
                panic!("receiver parameters are not supported")
            }
        }
        list
    };
    if sig.variadic.is_some() {
        panic!("variadic parameters are not supported");
    }
    let output = sig.output.clone();
    let mut versions = quote::quote! {};
    let mut branches = quote::quote! {};
    for version in attr.versions {
        let target = version.target.clone();
        let name = syn::Ident::new(
            &format!("{name}_{}", target.replace(":", "_").replace(".", "_")),
            proc_macro2::Span::mixed_site(),
        );
        let s = target.split(":").collect::<Vec<&str>>();
        let target_cpu = target::TARGET_CPUS
            .iter()
            .find(|target_cpu| target_cpu.target_cpu == s[0])
            .expect("unknown target_cpu");
        let additional_target_features = s[1..].to_vec();
        let target_arch = target_cpu.target_arch;
        let target_cpu = target_cpu.target_cpu;
        if !version.import {
            versions.extend(quote::quote! {
                #[inline]
                #[cfg(any(target_arch = #target_arch))]
                #[crate::simd::target_cpu(enable = #target_cpu)]
                #(#[target_feature(enable = #additional_target_features)])*
                fn #name < #generics_params > (#inputs) #output #generics_where { #block }
            });
        }
        branches.extend(quote::quote! {
            #[cfg(target_arch = #target_arch)]
            if crate::simd::is_cpu_detected!(#target_cpu) #(&& crate::simd::is_feature_detected!(#additional_target_features))* {
                let _multiversion_internal: unsafe fn(#inputs) #output = #name;
                CACHE.store(_multiversion_internal as *mut (), core::sync::atomic::Ordering::Relaxed);
                return unsafe { _multiversion_internal(#(#arguments,)*) };
            }
        });
    }
    let fallback_name =
        syn::Ident::new(&format!("{name}_fallback"), proc_macro2::Span::mixed_site());
    quote::quote! {
        #versions
        fn #fallback_name < #generics_params > (#inputs) #output #generics_where { #block }
        #[inline(always)]
        #(#attrs)* #vis #sig {
            static CACHE: core::sync::atomic::AtomicPtr<()> = core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());
            let cache = CACHE.load(core::sync::atomic::Ordering::Relaxed);
            if !cache.is_null() {
                let f = unsafe { core::mem::transmute::<*mut (), unsafe fn(#inputs) #output>(cache as _) };
                return unsafe { f(#(#arguments,)*) };
            }
            #branches
            let _multiversion_internal: unsafe fn(#inputs) #output = #fallback_name;
            CACHE.store(_multiversion_internal as *mut (), core::sync::atomic::Ordering::Relaxed);
            unsafe { _multiversion_internal(#(#arguments,)*) }
        }
    }
    .into()
}

struct TargetCpu {
    enable: String,
}

impl syn::parse::Parse for TargetCpu {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let _: syn::Ident = input.parse()?;
        let _: syn::Token![=] = input.parse()?;
        let enable: syn::LitStr = input.parse()?;
        Ok(Self {
            enable: enable.value(),
        })
    }
}

#[proc_macro_attribute]
pub fn target_cpu(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let attr = syn::parse_macro_input!(attr as TargetCpu);
    let mut result = quote::quote! {};
    for s in attr.enable.split(',') {
        let target_cpu = target::TARGET_CPUS
            .iter()
            .find(|target_cpu| target_cpu.target_cpu == s)
            .expect("unknown target_cpu");
        let target_features = target_cpu.target_features;
        result.extend(quote::quote!(
            #(#[target_feature(enable = #target_features)])*
        ));
    }
    result.extend(proc_macro2::TokenStream::from(item));
    result.into()
}

#[proc_macro]
pub fn define_is_cpu_detected(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let target_arch = syn::parse_macro_input!(input as syn::LitStr).value();
    let mut arms = quote::quote! {};
    for target_cpu in target::TARGET_CPUS {
        if target_cpu.target_arch != target_arch {
            continue;
        }
        let target_features = target_cpu.target_features;
        let target_cpu = target_cpu.target_cpu;
        arms.extend(quote::quote! {
            (#target_cpu) => {
                true #(&& crate::simd::is_feature_detected!(#target_features))*
            };
        });
    }
    let name = syn::Ident::new(
        &format!("is_{target_arch}_cpu_detected"),
        proc_macro2::Span::mixed_site(),
    );
    quote::quote! {
        #[macro_export]
        macro_rules! #name {
            #arms
        }
    }
    .into()
}
