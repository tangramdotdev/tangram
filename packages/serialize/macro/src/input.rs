use crate::{enum_::Enum, struct_::Struct};

pub enum Input<'a> {
	Struct(Struct<'a>),
	Enum(Enum<'a>),
}

impl<'a> Input<'a> {
	pub fn parse(input: &'a syn::DeriveInput) -> syn::Result<Input<'a>> {
		let data = &input.data;
		let parsed_input = match data {
			syn::Data::Struct(data) => Input::Struct(Struct::parse(input, data)?),
			syn::Data::Enum(data) => Input::Enum(Enum::parse(input, data)?),
			syn::Data::Union(_) => {
				let message = "This macro cannot be used on unions.";
				return Err(syn::Error::new_spanned(input, message));
			},
		};
		Ok(parsed_input)
	}

	pub fn serialize(self) -> proc_macro2::TokenStream {
		match self {
			Input::Struct(s) => s.serialize(),
			Input::Enum(e) => e.serialize(),
		}
	}

	pub fn deserialize(self) -> proc_macro2::TokenStream {
		match self {
			Input::Struct(s) => s.deserialize(),
			Input::Enum(e) => e.deserialize(),
		}
	}
}
