use super::Enum;
use itertools::Itertools;
use quote::quote;

impl Enum<'_> {
	pub fn deserialize(self) -> proc_macro2::TokenStream {
		// Get the ident.
		let ident = self.ident;

		// Generate the body.
		let body = if let Some(try_from) = self.try_from {
			quote! {
				let value = deserializer.deserialize::<#try_from>()?;
				let value = value.try_into().map_err(|error| ::std::io::Error::new(::std::io::ErrorKind::Other, error))?;
				Ok(value)
			}
		} else {
			// Get the variant ids.
			let variant_ids = self.variants.iter().map(|variant| variant.id).collect_vec();

			// Get the variant idents.
			let variant_idents = self
				.variants
				.iter()
				.map(|variant| &variant.ident)
				.collect_vec();

			quote! {
				// Read the kind.
				deserializer.ensure_kind(tangram_serialize::Kind::Enum)?;

				// Read the variant ID.
				let variant_id = deserializer.read_id()?;

				// Deserialize the value.
				let value = match variant_id {
					// Deserialize the variant's value.
					#(#variant_ids => #ident::#variant_idents(deserializer.deserialize()?),)*

					// Skip over variants with unknown ids.
					_ => {
						deserializer.deserialize::<tangram_serialize::Value>()?;
						return ::std::result::Result::Err(::std::io::Error::new(::std::io::ErrorKind::Other, "Unexpected variant ID."));
					},
				};

				Ok(value)
			}
		};

		// Generate the code.
		let code = quote! {
			impl tangram_serialize::Deserialize for #ident {
				fn deserialize<R>(deserializer: &mut tangram_serialize::Deserializer<R>) -> ::std::io::Result<Self>
				where
					R: ::std::io::Read,
				{
					#body
				}
			}
		};

		code
	}
}
