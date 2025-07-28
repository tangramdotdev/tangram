use super::Struct;
use itertools::Itertools as _;

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
		} else if self.display {
			quote! {
				let display_string = self.to_string();
				serializer.serialize(&display_string)?;
				Ok(())
			}
		} else if self.transparent {
			let fields = self.fields.iter().filter(|field| !field.skip).collect_vec();

			if fields.len() != 1 {
				return syn::Error::new_spanned(
					self.ident,
					"transparent can only be used on structs with exactly one field",
				)
				.to_compile_error();
			}

			let field = fields[0];

			let field_access = if let Some(field_ident) = field.ident {
				quote! { self.#field_ident }
			} else {
				let index = syn::Index::from(0);
				quote! { self.#index }
			};

			if let Some(serialize_with) = field.serialize_with.as_ref() {
				let serialize_with = quote::format_ident!("{serialize_with}");
				quote! {
					#serialize_with(&#field_access, serializer)?;
					Ok(())
				}
			} else if field.display {
				quote! {
					let display_string = (#field_access).to_string();
					serializer.serialize(&display_string)?;
					Ok(())
				}
			} else {
				quote! {
					serializer.serialize(&#field_access)?;
					Ok(())
				}
			}
		} else {
			// Get the fields.
			let fields = self.fields.iter().filter(|field| !field.skip).collect_vec();

			// Get the field ids.
			let field_ids = fields.iter().map(|field| field.id).collect_vec();

			let skip_conditions = fields
				.iter()
				.enumerate()
				.map(|(index, field)| {
					if let Some(skip_fn) = &field.skip_serializing_if {
						let skip_fn_path: syn::Path = syn::parse_str(skip_fn)
							.expect("Invalid skip_serializing_if function path");
						let field_access = if let Some(field_ident) = field.ident {
							quote! { &self.#field_ident }
						} else {
							// For tuple structs, use numeric index
							let index = syn::Index::from(index);
							quote! { &self.#index }
						};
						quote! { #skip_fn_path(#field_access) }
					} else {
						quote! { false }
					}
				})
				.collect_vec();

			let field_writes = fields
				.iter()
				.enumerate()
				.map(|(index, field)| {
					let field_access = if let Some(field_ident) = field.ident {
						quote! { self.#field_ident }
					} else {
						let index = syn::Index::from(index);
						quote! { self.#index }
					};

					if let Some(serialize_with) = field.serialize_with.as_ref() {
						let serialize_with = quote::format_ident!("{serialize_with}");
						quote! {
							#serialize_with(&#field_access, serializer)?;
						}
					} else if field.display {
						quote! {
							let display_string = (#field_access).to_string();
							serializer.serialize(&display_string)?;
						}
					} else {
						quote! {
							serializer.serialize(&#field_access)?;
						}
					}
				})
				.collect_vec();

			quote! {
				// Write the kind.
				serializer.write_kind(tangram_serialize::Kind::Struct)?;

				// Count the fields that will be serialized.
				let mut field_count = 0u64;
				#(
					if !(#skip_conditions) {
						field_count += 1;
					}
				)*

				// Write the number of fields.
				serializer.write_uvarint(field_count)?;

				// Write the fields.
				#(
					if !(#skip_conditions) {
						// Write the field ID.
						serializer.write_id(#field_ids)?;
						// Write the field value.
						#field_writes
					}
				)*

				Ok(())
			}
		};

		// Handle generics.
		let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();

		// Add Serialize bounds for all type parameters.
		let mut where_clause = where_clause.cloned().unwrap_or_else(|| syn::WhereClause {
			where_token: syn::token::Where::default(),
			predicates: syn::punctuated::Punctuated::new(),
		});

		// Add the Serialize bound for each type parameter.
		for param in &self.generics.params {
			if let syn::GenericParam::Type(type_param) = param {
				let ident = &type_param.ident;
				let predicate: syn::WherePredicate = syn::parse_quote! {
					#ident: tangram_serialize::Serialize
				};
				where_clause.predicates.push(predicate);
			}
		}

		// Generate the code.
		let code = quote! {
			impl #impl_generics tangram_serialize::Serialize for #ident #ty_generics
			#where_clause
			{
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
