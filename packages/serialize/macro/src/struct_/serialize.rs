use super::Struct;
use itertools::Itertools;
use num::ToPrimitive;
use quote::quote;

impl Struct<'_> {
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
			// Get the fields.
			let fields = self.fields.iter().filter(|field| !field.skip).collect_vec();

			// Get the len.
			let len = fields.len().to_u8().unwrap();

			// Get the field ids.
			let field_ids = fields.iter().map(|field| field.id).collect_vec();

			// Create the field writes.
			let field_writes = fields
				.iter()
				.map(|field| {
					let ident = field.ident.unwrap();
					if let Some(serialize_with) = field.serialize_with.as_ref() {
						let serialize_with = quote::format_ident!("{serialize_with}");
						quote! {
							#serialize_with(&self.#ident, serializer)?;
						}
					} else {
						quote! {
							serializer.serialize(&self.#ident)?;
						}
					}
				})
				.collect_vec();

			quote! {
				// Write the kind.
				serializer.write_kind(tangram_serialize::Kind::Struct)?;

				// Write the number of fields.
				serializer.write_uvarint(#len as u64)?;

				// Write the fields.
				#(
					// Write the field ID.
					serializer.write_id(#field_ids)?;
					// Write the field value.
					#field_writes
				)*;

				Ok(())
			}
		};

		// Generate the code.
		let code = quote! {
			impl tangram_serialize::Serialize for #ident {
				fn serialize<W>(&self, serializer: &mut tangram_serialize::Serializer<W>) -> std::io::Result<()>
				where
					W: std::io::Write,
				{
					#body
				}
			}
		};

		code
	}
}
