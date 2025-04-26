use super::Enum;
use itertools::Itertools;
use quote::quote;

impl Enum<'_> {
	pub fn serialize(self) -> proc_macro2::TokenStream {
		// Get the ident.
		let ident = self.ident;

		// Generate the body.
		let body = if let Some(into) = self.into {
			quote! {
				// Convert the value.
				let s: #into = self.clone().into();

				// Serialize the value.
				tangram_serialize::Serialize::serialize(&s, serializer)?;

				Ok(())
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
				match self {
					#(#ident::#variant_idents(value) => {
						// Write the kind.
						serializer.write_kind(tangram_serialize::Kind::Enum)?;

						// Serialize the variant ID.
						serializer.write_id(#variant_ids)?;

						// Serialize the value.
						serializer.serialize(value)?;
					})*
				};
				Ok(())
			}
		};

		// Generate the code.
		let code = quote! {
			impl tangram_serialize::Serialize for #ident {
				fn serialize<W>(&self, serializer: &mut tangram_serialize::Serializer<W>) -> ::std::io::Result<()>
				where
					W: ::std::io::Write,
				{
					#body
				}
			}
		};

		code
	}
}
