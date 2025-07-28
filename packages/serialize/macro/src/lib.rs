use self::input::Input;

mod enum_;
mod input;
mod struct_;

#[proc_macro_derive(Serialize, attributes(tangram_serialize))]
pub fn serialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	serialize_inner(input.into())
		.unwrap_or_else(|e| e.to_compile_error())
		.into()
}

fn serialize_inner(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
	let input: syn::DeriveInput = syn::parse2(input)?;
	let input = Input::parse(&input)?;
	let code = input.serialize();
	Ok(code)
}

#[proc_macro_derive(Deserialize, attributes(tangram_serialize))]
pub fn deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	deserialize_inner(input.into())
		.unwrap_or_else(|e| e.to_compile_error())
		.into()
}

fn deserialize_inner(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
	let input: syn::DeriveInput = syn::parse2(input)?;
	let input = Input::parse(&input)?;
	let code = input.deserialize();
	Ok(code)
}
