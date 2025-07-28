use super::{Enum, VariantKind};
use itertools::Itertools as _;
use quote::quote;

impl Enum<'_> {
	pub fn deserialize(self) -> proc_macro2::TokenStream {
		// Get the ident.
		let ident = self.ident;

		// Generate the body.
		let body = if self.from_str {
			quote! {
				let display_string: String = deserializer.deserialize()?;
				let value = display_string.parse().map_err(|error| ::std::io::Error::new(::std::io::ErrorKind::Other, format!("{}", error)))?;
				Ok(value)
			}
		} else if let Some(try_from) = self.try_from {
			quote! {
				let value = deserializer.deserialize::<#try_from>()?;
				let value = value.try_into().map_err(|error| ::std::io::Error::new(::std::io::ErrorKind::Other, error))?;
				Ok(value)
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
			let tuple_field_deserializations = tuple_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Tuple(types) = &variant.kind {
						let field_deserializations = types
							.iter()
							.map(|_| quote! { deserializer.deserialize()? })
							.collect_vec();
						quote! { ( #(#field_deserializations),* ) }
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
			let struct_field_deserializations = struct_variants
				.iter()
				.map(|variant| {
					if let VariantKind::Struct(fields) = &variant.kind {
						let field_assignments = fields
							.iter()
							.map(|field| {
								let field_name = &field.ident;
								quote! { #field_name: deserializer.deserialize()? }
							})
							.collect_vec();
						quote! { { #(#field_assignments),* } }
					} else {
						unreachable!()
					}
				})
				.collect_vec();

			quote! {
				// Read the kind.
				deserializer.ensure_kind(tangram_serialize::Kind::Enum)?;

				// Read the variant ID.
				let variant_id = deserializer.read_id()?;

				// Deserialize the value.
				let value = match variant_id {
					// Deserialize unit variants.
					#(#unit_variant_ids => {
						deserializer.deserialize::<()>()?;
						#ident::#unit_variant_idents
					})*

					// Deserialize tuple variants.
					#(#tuple_variant_ids => {
						#ident::#tuple_variant_idents #tuple_field_deserializations
					})*

					// Deserialize struct variants.
					#(#struct_variant_ids => {
						#ident::#struct_variant_idents #struct_field_deserializations
					})*

					// Skip over variants with unknown ids.
					_ => {
						deserializer.deserialize::<tangram_serialize::Value>()?;
						return ::std::result::Result::Err(::std::io::Error::new(::std::io::ErrorKind::Other, "Unexpected variant ID."));
					},
				};

				Ok(value)
			}
		};

		// Handle generics.
		let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();

		// Add Deserialize bounds for all type parameters.
		let mut where_clause = where_clause.cloned().unwrap_or_else(|| syn::WhereClause {
			where_token: syn::token::Where::default(),
			predicates: syn::punctuated::Punctuated::new(),
		});

		// Add the Deserialize bound for each type parameter.
		for param in &self.generics.params {
			if let syn::GenericParam::Type(type_param) = param {
				let ident = &type_param.ident;
				let predicate: syn::WherePredicate = syn::parse_quote! {
					#ident: tangram_serialize::Deserialize
				};
				where_clause.predicates.push(predicate);
			}
		}

		// Generate the code.
		let code = quote! {
			impl #impl_generics tangram_serialize::Deserialize for #ident #ty_generics
			#where_clause
			{
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
