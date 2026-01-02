use {
	crate::{Enum, Item, Struct, VariantKind},
	itertools::Itertools as _,
	quote::quote,
};

impl Item<'_> {
	pub fn deserialize(self) -> proc_macro2::TokenStream {
		match self {
			Item::Struct(s) => s.deserialize(),
			Item::Enum(e) => e.deserialize(),
		}
	}
}

impl Struct<'_> {
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

			let skipped_fields = self.fields.iter().filter(|field| field.skip).collect_vec();
			let skipped_field_idents = skipped_fields
				.iter()
				.map(|field| &field.ident)
				.collect_vec();

			let struct_construction = if let Some(field_ident) = field.ident {
				quote! {
					#ident {
						#field_ident: field_value,
						#(#skipped_field_idents: ::std::default::Default::default(),)*
					}
				}
			} else {
				quote! {
					#ident(field_value)
				}
			};

			if let Some(deserialize_with) = field.deserialize_with.as_ref() {
				let deserialize_with = quote::format_ident!("{deserialize_with}");
				quote! {
					let field_value = #deserialize_with(deserializer)?;
					Ok(#struct_construction)
				}
			} else if field.from_str {
				quote! {
					let display_string: String = deserializer.deserialize()?;
					let field_value = display_string.parse().map_err(|error| ::std::io::Error::new(::std::io::ErrorKind::Other, format!("{}", error)))?;
					Ok(#struct_construction)
				}
			} else {
				quote! {
					let field_value = deserializer.deserialize()?;
					Ok(#struct_construction)
				}
			}
		} else {
			// Get the fields.
			let fields = self.fields.iter().filter(|field| !field.skip).collect_vec();

			// Get the field ids.
			let field_ids = fields.iter().map(|field| field.id).collect_vec();

			// Get the field idents.
			let field_idents = fields.iter().map(|field| &field.ident).collect_vec();

			// Separate fields into those with and without default attribute.
			let fields_with_default = fields
				.iter()
				.filter(|field| field.default.is_some())
				.collect_vec();
			let fields_without_default = fields
				.iter()
				.filter(|field| field.default.is_none())
				.collect_vec();

			let non_default_field_idents = fields_without_default
				.iter()
				.map(|field| &field.ident)
				.collect_vec();

			// Generate individual default assignment statements.
			let default_field_statements = fields_with_default
				.iter()
				.map(|field| {
					let field_ident = field.ident.as_ref().unwrap();
					let default_path = field.default.as_ref().unwrap();
					let default_call = if default_path.is_empty() {
						// Use Default::default() if no custom function specified.
						quote! { ::std::default::Default::default() }
					} else {
						// Use custom function path.
						let path: proc_macro2::TokenStream = default_path.parse().unwrap();
						quote! { #path() }
					};
					quote! {
						let #field_ident = if let Some(value) = #field_ident {
							value
						} else {
							#default_call
						};
					}
				})
				.collect_vec();

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
					} else if field.from_str {
						quote! {
							{
								let display_string: String = deserializer.deserialize()?;
								display_string.parse().map_err(|error| ::std::io::Error::new(::std::io::ErrorKind::Other, format!("{}", error)))?
							}
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

				// Handle default field assignments.
				#(#default_field_statements)*

				#(let #non_default_field_idents = #non_default_field_idents.ok_or_else(|| ::std::io::Error::new(::std::io::ErrorKind::Other, "Missing field."))?;)*

				// Create the struct.
				Ok(#ident {
					#(#field_idents,)*
					#(#skipped_field_idents: ::std::default::Default::default(),)*
				})
			}
		};

		// Handle generics.
		let (_, ty_generics, where_clause) = self.generics.split_for_impl();

		// Add Deserialize bounds for all type parameters.
		let mut where_clause = where_clause.cloned().unwrap_or_else(|| syn::WhereClause {
			where_token: syn::token::Where::default(),
			predicates: syn::punctuated::Punctuated::new(),
		});

		// Collect the generic parameters (without angle brackets).
		let generic_params = &self.generics.params;

		// Add the Deserialize<'de> bound for each type parameter.
		for param in generic_params {
			if let syn::GenericParam::Type(type_param) = param {
				let ident = &type_param.ident;
				let predicate: syn::WherePredicate = syn::parse_quote! {
					#ident: tangram_serialize::Deserialize<'de>
				};
				where_clause.predicates.push(predicate);
			}
		}

		// Generate the code with 'de lifetime.
		if generic_params.is_empty() {
			quote! {
				impl<'de> tangram_serialize::Deserialize<'de> for #ident #ty_generics
				#where_clause
				{
					fn deserialize(deserializer: &mut tangram_serialize::Deserializer<'de>) -> ::std::io::Result<Self>
					{
						#body
					}
				}
			}
		} else {
			quote! {
				impl<'de, #generic_params> tangram_serialize::Deserialize<'de> for #ident #ty_generics
				#where_clause
				{
					fn deserialize(deserializer: &mut tangram_serialize::Deserializer<'de>) -> ::std::io::Result<Self>
					{
						#body
					}
				}
			}
		}
	}
}

impl Enum<'_> {
	pub fn deserialize(self) -> proc_macro2::TokenStream {
		// Get the ident.
		let ident = self.ident;

		// Generate the body.
		let body = if self.untagged {
			// For untagged enums, try each variant in order with backtracking.
			let variant_attempts = self
				.variants
				.iter()
				.map(|variant| {
					let variant_ident = variant.ident;
					match &variant.kind {
						VariantKind::Unit => {
							quote! {
								deserializer.seek(start)?;
								match deserializer.deserialize::<()>() {
									::std::result::Result::Ok(_) => {
										return ::std::result::Result::Ok(#ident::#variant_ident);
									},
									::std::result::Result::Err(_) => {},
								}
							}
						},
						VariantKind::Tuple(types) => {
							let field_names = (0..types.len())
								.map(|i| quote::format_ident!("field_{}", i))
								.collect_vec();
							let field_deserializations = field_names
								.iter()
								.map(|field_name| {
									quote! {
										let #field_name = match deserializer.deserialize() {
											::std::result::Result::Ok(value) => value,
											::std::result::Result::Err(_) => break,
										};
									}
								})
								.collect_vec();
							quote! {
								deserializer.seek(start)?;
								loop {
									#(#field_deserializations)*
									return ::std::result::Result::Ok(#ident::#variant_ident(#(#field_names),*));
								}
							}
						},
						VariantKind::Struct(fields) => {
							let field_names = fields.iter().map(|f| f.ident).collect_vec();
							let field_deserializations = field_names
								.iter()
								.map(|field_name| {
									quote! {
										let #field_name = match deserializer.deserialize() {
											::std::result::Result::Ok(value) => value,
											::std::result::Result::Err(_) => break,
										};
									}
								})
								.collect_vec();
							quote! {
								deserializer.seek(start)?;
								loop {
									#(#field_deserializations)*
									return ::std::result::Result::Ok(#ident::#variant_ident { #(#field_names),* });
								}
							}
						},
					}
				})
				.collect_vec();

			quote! {
				let start = deserializer.position();
				#(#variant_attempts)*
				::std::result::Result::Err(::std::io::Error::new(::std::io::ErrorKind::Other, "No variant matched for untagged enum"))
			}
		} else if self.from_str {
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
		let (_, ty_generics, where_clause) = self.generics.split_for_impl();

		// Add Deserialize bounds for all type parameters.
		let mut where_clause = where_clause.cloned().unwrap_or_else(|| syn::WhereClause {
			where_token: syn::token::Where::default(),
			predicates: syn::punctuated::Punctuated::new(),
		});

		// Collect the generic parameters (without angle brackets).
		let generic_params = &self.generics.params;

		// Add the Deserialize<'de> bound for each type parameter.
		for param in generic_params {
			if let syn::GenericParam::Type(type_param) = param {
				let ident = &type_param.ident;
				let predicate: syn::WherePredicate = syn::parse_quote! {
					#ident: tangram_serialize::Deserialize<'de>
				};
				where_clause.predicates.push(predicate);
			}
		}

		// Generate the code with 'de lifetime.
		if generic_params.is_empty() {
			quote! {
				impl<'de> tangram_serialize::Deserialize<'de> for #ident #ty_generics
				#where_clause
				{
					fn deserialize(deserializer: &mut tangram_serialize::Deserializer<'de>) -> ::std::io::Result<Self>
					{
						#body
					}
				}
			}
		} else {
			quote! {
				impl<'de, #generic_params> tangram_serialize::Deserialize<'de> for #ident #ty_generics
				#where_clause
				{
					fn deserialize(deserializer: &mut tangram_serialize::Deserializer<'de>) -> ::std::io::Result<Self>
					{
						#body
					}
				}
			}
		}
	}
}
