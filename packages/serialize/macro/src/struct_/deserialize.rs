use super::Struct;
use itertools::Itertools;
use quote::quote;

impl Struct<'_> {
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
			// Get the fields.
			let fields = self.fields.iter().filter(|field| !field.skip).collect_vec();

			// Get the field ids.
			let field_ids = fields.iter().map(|field| field.id).collect_vec();

			// Get the field idents.
			let field_idents = fields.iter().map(|field| &field.ident).collect_vec();

			// Get the skipped fields.
			let skipped_fields = self.fields.iter().filter(|field| field.skip).collect_vec();

			// Get the field idents.
			let skipped_field_idents = skipped_fields
				.iter()
				.map(|field| &field.ident)
				.collect_vec();

			// Create the field reads.
			let field_reads = fields
				.iter()
				.map(|field| {
					if let Some(deserialize_with) = field.deserialize_with.as_ref() {
						let deserialize_with = quote::format_ident!("{deserialize_with}");
						quote! {
							#deserialize_with(deserializer)?
						}
					} else {
						quote! {
							deserializer.deserialize()?
						}
					}
				})
				.collect_vec();

			quote! {
				// Read the kind.
				deserializer.ensure_kind(tangram_serialize::Kind::Struct)?;

				// Initialize the fields.
				#(let mut #field_idents = None;)*

				// Read the number of serialized fields.
				let len = deserializer.read_uvarint()?;

				// Deserialize `len` fields.
				for _ in 0..len {
					// Deserialize the field ID.
					let field_id = deserializer.read_id()?;

					// Deserialize the field value.
					match field_id {
						#(#field_ids => { #field_idents = ::std::option::Option::Some(#field_reads); })*

						// Skip over fields with unknown ids.
						_ => {
							tangram_serialize::Value::deserialize(deserializer)?;
						},
					}
				}

				// Retrieve the fields.
				#(let #field_idents = #field_idents.ok_or_else(|| ::std::io::Error::new(::std::io::ErrorKind::Other, "Missing field."))?;)*

				// Create the struct.
				Ok(#ident {
					#(#field_idents,)*
					#(#skipped_field_idents: ::std::default::Default::default(),)*
				})
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
