mod deserialize;
mod parse;
mod serialize;

pub struct Enum<'a> {
	pub display: bool,
	pub from_str: bool,
	pub ident: &'a syn::Ident,
	pub generics: &'a syn::Generics,
	pub into: Option<syn::Type>,
	pub try_from: Option<syn::Type>,
	pub variants: Vec<Variant<'a>>,
}

pub struct Variant<'a> {
	pub id: Option<u8>,
	pub ident: &'a syn::Ident,
	pub kind: VariantKind<'a>,
}

#[derive(Clone)]
pub enum VariantKind<'a> {
	Unit,
	Tuple(Vec<&'a syn::Type>),
	Struct(Vec<StructVariantField<'a>>),
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct StructVariantField<'a> {
	pub ident: &'a syn::Ident,
	pub ty: &'a syn::Type,
}
