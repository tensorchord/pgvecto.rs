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
