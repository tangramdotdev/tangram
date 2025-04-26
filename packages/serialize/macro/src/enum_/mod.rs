mod deserialize;
mod parse;
mod serialize;

pub struct Enum<'a> {
	pub ident: &'a syn::Ident,
	pub into: Option<syn::Type>,
	pub try_from: Option<syn::Type>,
	pub variants: Vec<Variant<'a>>,
}

pub struct Variant<'a> {
	pub id: Option<u8>,
	pub ident: &'a syn::Ident,
}
