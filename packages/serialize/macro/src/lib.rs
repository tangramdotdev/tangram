mod deserialize;
mod parse;
mod serialize;

#[proc_macro_derive(Serialize, attributes(tangram_serialize))]
pub fn serialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	serialize_inner(input.into())
		.unwrap_or_else(|e| e.to_compile_error())
		.into()
}

fn serialize_inner(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
	let input: syn::DeriveInput = syn::parse2(input)?;
	let item = Item::parse(&input)?;
	let code = item.serialize();
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
	let item = Item::parse(&input)?;
	let code = item.deserialize();
	Ok(code)
}

enum Item<'a> {
	Struct(Struct<'a>),
	Enum(Enum<'a>),
}

struct Struct<'a> {
	display: bool,
	fields: Vec<Field<'a>>,
	from_str: bool,
	generics: &'a syn::Generics,
	ident: &'a syn::Ident,
	into: Option<syn::Type>,
	transparent: bool,
	try_from: Option<syn::Type>,
}

struct Field<'a> {
	default: bool,
	deserialize_with: Option<String>,
	display: bool,
	from_str: bool,
	id: Option<u8>,
	ident: Option<&'a syn::Ident>,
	serialize_with: Option<String>,
	skip: bool,
	skip_serializing_if: Option<String>,
}

struct Enum<'a> {
	display: bool,
	from_str: bool,
	generics: &'a syn::Generics,
	ident: &'a syn::Ident,
	into: Option<syn::Type>,
	try_from: Option<syn::Type>,
	variants: Vec<Variant<'a>>,
}

struct Variant<'a> {
	id: Option<u8>,
	ident: &'a syn::Ident,
	kind: VariantKind<'a>,
}

#[derive(Clone)]
enum VariantKind<'a> {
	Unit,
	Tuple(Vec<&'a syn::Type>),
	Struct(Vec<StructVariantField<'a>>),
}

#[derive(Clone)]
#[allow(dead_code)]
struct StructVariantField<'a> {
	ident: &'a syn::Ident,
	ty: &'a syn::Type,
}
