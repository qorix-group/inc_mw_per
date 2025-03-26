use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, Fields, GenericArgument};

/// KvsValue Derive Macro
#[proc_macro_derive(KvsValue)]
pub fn derive_rt_kvs_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let input_name = &input.ident;

    if let syn::Data::Struct(ref data) = input.data {
        let mut result = format!("impl {input_name} {{\n");
        result.push_str("pub fn create(kvs: &Kvs) -> Result<Self, ErrorCode> {\n");
        result.push_str("Ok(Self {\n");

        if let Fields::Named(ref fields) = data.fields {
            for field in fields.named.iter() {
                let field_name = field.ident.clone().expect("Field has no name").to_string();
                let mut field_type = String::new();

                // check if field has type `KvsValue` and extract it's inner type
                if let syn::Type::Path(path) = &field.ty {
                    let segment = &path.path.segments[0];
                    if segment.ident == "KvsValue" {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let GenericArgument::Type(syn::Type::Path(path)) = &args.args[0] {
                                field_type = path.path.segments[0].ident.to_string();
                            }
                        }
                    }
                }

                if field_type.is_empty() {
                    result.push_str(&format!("{field_name}: Default::default(),\n"));
                    continue;
                }

                result.push_str(&format!(
                    "{field_name}: kvs.get_value_object::<{field_type}>(\"{field_name}\")?,\n"
                ));
            }
        }

        result.push_str("})}}");
        return result.parse::<TokenStream>().unwrap();
    }

    TokenStream::from(
        syn::Error::new(
            input.ident.span(),
            "Only structs with named fields can derive `KvsValue`",
        )
        .to_compile_error(),
    )
}
