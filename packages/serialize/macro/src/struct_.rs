mod deserialize;
mod parse;
mod serialize;

pub struct Struct<'a> {
	pub display: bool,
	pub fields: Vec<Field<'a>>,
	pub from_str: bool,
	pub ident: &'a syn::Ident,
	pub generics: &'a syn::Generics,
	pub into: Option<syn::Type>,
	pub transparent: bool,
	pub try_from: Option<syn::Type>,
}

pub struct Field<'a> {
	pub deserialize_with: Option<String>,
	pub display: bool,
	pub from_str: bool,
	pub id: Option<u8>,
	pub ident: Option<&'a syn::Ident>,
	pub serialize_with: Option<String>,
	pub skip: bool,
	pub default: bool,
	pub skip_serializing_if: Option<String>,
}
