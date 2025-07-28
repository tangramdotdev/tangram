use super::{Enum, VariantKind};
use itertools::Itertools as _;
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
		} else if self.display {
			quote! {
				let display_string = self.to_string();
				serializer.serialize(&display_string)?;
				Ok(())
			}
		} else {
			// Separate variants by type.
			let unit_variants = self
				.variants
				.iter()
				.filter(|variant| matches!(variant.kind, VariantKind::Unit))
				.collect_vec();
			let tuple_variants = self
				.variants
				.iter()
				.filter(|variant| matches!(variant.kind, VariantKind::Tuple(_)))
				.collect_vec();
			let struct_variants = self
				.variants
				.iter()
				.filter(|variant| matches!(variant.kind, VariantKind::Struct(_)))
				.collect_vec();

			// Get data for unit variants.
			let unit_variant_ids = unit_variants.iter().map(|variant| variant.id).collect_vec();
			let unit_variant_idents = unit_variants
				.iter()
				.map(|variant| &variant.ident)
				.collect_vec();

			// Get data for tuple variants.
			let tuple_variant_ids = tuple_variants
				.iter()
				.map(|variant| variant.id)
				.collect_vec();
			let tuple_variant_idents = tuple_variants
				.iter()
				.map(|variant| &variant.ident)
				.collect_vec();
			let tuple_field_patterns = tuple_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Tuple(types) = &variant.kind {
						let field_names = (0..types.len())
							.map(|i| {
								syn::Ident::new(
									&format!("field_{i}"),
									proc_macro2::Span::call_site(),
								)
							})
							.collect_vec();
						quote! { ( #(#field_names),* ) }
					} else {
						unreachable!()
					}
				})
				.collect_vec();
			let tuple_field_serializations = tuple_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Tuple(types) = &variant.kind {
						let field_names = (0..types.len())
							.map(|i| {
								syn::Ident::new(
									&format!("field_{i}"),
									proc_macro2::Span::call_site(),
								)
							})
							.collect_vec();
						quote! {
							#(serializer.serialize(#field_names)?;)*
						}
					} else {
						unreachable!()
					}
				})
				.collect_vec();

			// Get data for struct variants.
			let struct_variant_ids = struct_variants
				.iter()
				.map(|variant| variant.id)
				.collect_vec();
			let struct_variant_idents = struct_variants
				.iter()
				.map(|variant| &variant.ident)
				.collect_vec();
			let struct_field_patterns = struct_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Struct(fields) = &variant.kind {
						let field_names = fields.iter().map(|field| &field.ident).collect_vec();
						quote! { { #(#field_names),* } }
					} else {
						unreachable!()
					}
				})
				.collect_vec();
			let struct_field_serializations = struct_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Struct(fields) = &variant.kind {
						let field_names = fields.iter().map(|field| &field.ident).collect_vec();
						quote! {
							#(serializer.serialize(#field_names)?;)*
						}
					} else {
						unreachable!()
					}
				})
				.collect_vec();

			quote! {
				match self {
					// Handle unit variants.
					#(#ident::#unit_variant_idents => {
						// Write the kind.
						serializer.write_kind(tangram_serialize::Kind::Enum)?;

						// Serialize the variant ID.
						serializer.write_id(#unit_variant_ids)?;

						// For unit variants, serialize unit value.
						serializer.serialize(&())?;
					})*

					// Handle tuple variants.
					#(#ident::#tuple_variant_idents #tuple_field_patterns => {
						// Write the kind.
						serializer.write_kind(tangram_serialize::Kind::Enum)?;

						// Serialize the variant ID.
						serializer.write_id(#tuple_variant_ids)?;

						// Serialize the tuple fields.
						#tuple_field_serializations
					})*

					// Handle struct variants.
					#(#ident::#struct_variant_idents #struct_field_patterns => {
						// Write the kind.
						serializer.write_kind(tangram_serialize::Kind::Enum)?;

						// Serialize the variant ID.
						serializer.write_id(#struct_variant_ids)?;

						// Serialize the struct fields.
						#struct_field_serializations
					})*
				};
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
