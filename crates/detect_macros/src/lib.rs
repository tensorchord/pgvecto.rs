struct List {
    target_cpu: &'static str,
    target_arch: &'static str,
    target_features: &'static str,
}

const LIST: &[List] = &[
    List {
        target_cpu: "v4",
        target_arch: "x86_64",
        target_features:
            "avx,avx2,avx512bw,avx512cd,avx512dq,avx512f,avx512vl,bmi1,bmi2,cmpxchg16b,f16c,fma,fxsr,lzcnt,movbe,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave"
    },
    List {
        target_cpu: "v3",
        target_arch: "x86_64",
        target_features:
            "avx,avx2,bmi1,bmi2,cmpxchg16b,f16c,fma,fxsr,lzcnt,movbe,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave"
    },
    List {
        target_cpu: "v2",
        target_arch: "x86_64",
        target_features: "cmpxchg16b,fxsr,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3",
    },
    List {
        target_cpu: "neon",
        target_arch: "aarch64",
        target_features: "neon",
    },
    List {
        target_cpu: "v4_avx512vpopcntdq",
        target_arch: "x86_64",
        target_features:
            "avx512vpopcntdq,avx,avx2,avx512bw,avx512cd,avx512dq,avx512f,avx512vl,bmi1,bmi2,cmpxchg16b,f16c,fma,fxsr,lzcnt,movbe,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave",
    },
    List {
        target_cpu: "v4_avx512fp16",
        target_arch: "x86_64",
        target_features:
            "avx512fp16,avx,avx2,avx512bw,avx512cd,avx512dq,avx512f,avx512vl,bmi1,bmi2,cmpxchg16b,f16c,fma,fxsr,lzcnt,movbe,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave",
    },
    List {
        target_cpu: "v4_avx512vnni",
        target_arch: "x86_64",
        target_features:
            "avx512vnni,avx,avx2,avx512bw,avx512cd,avx512dq,avx512f,avx512vl,bmi1,bmi2,cmpxchg16b,f16c,fma,fxsr,lzcnt,movbe,popcnt,sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave",
    },
];

enum MultiversionPort {
    Import,
    Export,
    Hidden,
}

struct MultiversionVersion {
    ident: String,
    // Some(false) => import (specialization)
    // Some(true) => export
    // None => hidden
    port: MultiversionPort,
}

impl syn::parse::Parse for MultiversionVersion {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        let lookahead1 = input.lookahead1();
        if lookahead1.peek(syn::Token![=]) {
            let _: syn::Token![=] = input.parse()?;
            let p: syn::Ident = input.parse()?;
            if p == "import" {
                Ok(Self {
                    ident: ident.to_string(),
                    port: MultiversionPort::Import,
                })
            } else if p == "export" {
                Ok(Self {
                    ident: ident.to_string(),
                    port: MultiversionPort::Export,
                })
            } else if p == "hidden" {
                Ok(Self {
                    ident: ident.to_string(),
                    port: MultiversionPort::Hidden,
                })
            } else {
                panic!("unknown port type")
            }
        } else {
            Ok(Self {
                ident: ident.to_string(),
                port: MultiversionPort::Hidden,
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
    let mut versions_export = quote::quote! {};
    let mut versions_hidden = quote::quote! {};
    let mut branches = quote::quote! {};
    let mut fallback = false;
    for version in attr.versions {
        let ident = version.ident.clone();
        let name = syn::Ident::new(&format!("{name}_{ident}"), proc_macro2::Span::mixed_site());
        let port;
        let branch;
        if fallback {
            panic!("fallback version is set");
        } else if ident == "fallback" {
            fallback = true;
            port = quote::quote! {
                unsafe fn #name < #generics_params > (#inputs) #output #generics_where { #block }
            };
            branch = quote::quote! {
                {
                    let _multiversion_internal: unsafe fn(#inputs) #output = #name;
                    CACHE.store(_multiversion_internal as *mut (), core::sync::atomic::Ordering::Relaxed);
                    unsafe { _multiversion_internal(#(#arguments,)*) }
                }
            };
        } else {
            let target_cpu = ident.clone();
            let t = syn::Ident::new(&target_cpu, proc_macro2::Span::mixed_site());
            let list = LIST
                .iter()
                .find(|list| list.target_cpu == target_cpu)
                .expect("unknown target_cpu");
            let target_arch = list.target_arch;
            let target_features = list.target_features;
            port = quote::quote! {
                #[cfg(any(target_arch = #target_arch, doc))]
                #[doc(cfg(target_arch = #target_arch))]
                #[target_feature(enable = #target_features)]
                unsafe fn #name < #generics_params > (#inputs) #output #generics_where { #block }
            };
            branch = quote::quote! {
                #[cfg(target_arch = #target_arch)]
                if detect::#t::detect() {
                    let _multiversion_internal: unsafe fn(#inputs) #output = #name;
                    CACHE.store(_multiversion_internal as *mut (), core::sync::atomic::Ordering::Relaxed);
                    return unsafe { _multiversion_internal(#(#arguments,)*) };
                }
            };
        }
        match version.port {
            MultiversionPort::Import => (),
            MultiversionPort::Export => versions_export.extend(port),
            MultiversionPort::Hidden => versions_hidden.extend(port),
        }
        branches.extend(branch);
    }
    if !fallback {
        panic!("fallback version is not set");
    }
    quote::quote! {
        #versions_export
        #[inline(always)]
        #(#attrs)* #vis #sig {
            #versions_hidden
            static CACHE: core::sync::atomic::AtomicPtr<()> = core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());
            let cache = CACHE.load(core::sync::atomic::Ordering::Relaxed);
            if !cache.is_null() {
                let f = unsafe { core::mem::transmute::<*mut (), unsafe fn(#inputs) #output>(cache as _) };
                return unsafe { f(#(#arguments,)*) };
            }
            #branches
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
    for cpu in attr.enable.split(',') {
        let list = LIST
            .iter()
            .find(|list| list.target_cpu == cpu)
            .expect("unknown target_cpu");
        let target_features = list.target_features;
        result.extend(quote::quote!(#[target_feature(enable = #target_features)]));
    }
    result.extend(proc_macro2::TokenStream::from(item));
    result.into()
}

#[proc_macro]
pub fn main(_: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut modules = quote::quote! {};
    let mut init = quote::quote! {};
    for x in LIST {
        let ident = syn::Ident::new(x.target_cpu, proc_macro2::Span::mixed_site());
        let target_cpu = x.target_cpu;
        let list = LIST
            .iter()
            .find(|list| list.target_cpu == target_cpu)
            .expect("unknown target_cpu");
        let target_arch = list.target_arch;
        let target_features = list.target_features.split(',').collect::<Vec<_>>();
        modules.extend(quote::quote! {
            #[cfg(target_arch = #target_arch)]
            pub mod #ident {
                use std::sync::atomic::{AtomicBool, Ordering};

                static ATOMIC: AtomicBool = AtomicBool::new(false);

                #[cfg(target_arch = "x86_64")]
                pub fn test() -> bool {
                    true #(&& std::arch::is_x86_feature_detected!(#target_features))*
                }

                #[cfg(target_arch = "aarch64")]
                pub fn test() -> bool {
                    true #(&& std::arch::is_aarch64_feature_detected!(#target_features))*
                }

                pub(crate) fn init() {
                    ATOMIC.store(test(), Ordering::Relaxed);
                }

                pub fn detect() -> bool {
                    ATOMIC.load(Ordering::Relaxed)
                }
            }
        });
        init.extend(quote::quote! {
            #[cfg(target_arch = #target_arch)]
            self::#ident::init();
        });
    }
    quote::quote! {
        #modules
        pub fn init() {
            #init
        }
    }
    .into()
}
