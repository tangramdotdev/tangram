use quote::quote;

struct FieldAttrs {
	try_from: Option<syn::Type>,
	deserialize_with: Option<syn::Path>,
	as_type: Option<syn::Type>,
}

#[proc_macro_derive(RowDeserialize, attributes(tangram_database))]
pub fn row_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	let name = &input.ident;

	let syn::Data::Struct(data) = &input.data else {
		return syn::Error::new_spanned(&input, "this can only be derived for structs")
			.to_compile_error()
			.into();
	};

	let syn::Fields::Named(fields) = &data.fields else {
		return syn::Error::new_spanned(
			&input,
			"this can only be derived for structs with named fields",
		)
		.to_compile_error()
		.into();
	};

	let field_exprs: Vec<_> = fields
		.named
		.iter()
		.map(|field| {
			let name = field.ident.as_ref().unwrap();
			let name_str = name.to_string();
			let field_type = &field.ty;
			let attrs = FieldAttrs::from_attrs(&field.attrs);

			if let Some(deserialize_with) = attrs.deserialize_with {
				quote! {
					#name: {
						#deserialize_with(&row, #name_str)
							.map_err(|error| format!("failed to deserialize column \"{error}\": {}", #name_str))?
					}
				}
			} else if let Some(as_type) = attrs.as_type {
				quote! {
					#name: {
						let value = row.get(tangram_either::Either::Right(#name_str))
							.ok_or_else(|| format!("missing column \"{}\"", #name_str))?
							.clone();
						<#as_type as tangram_database::value::DeserializeAs<#field_type>>::deserialize_as(value)
							.map_err(|error| format!("failed to deserialize column \"{}\": {error}", #name_str))?
					}
				}
			} else if let Some(try_from) = attrs.try_from {
				quote! {
					#name: {
						let value = row.get(tangram_either::Either::Right(#name_str))
							.ok_or_else(|| format!("missing column \"{}\"", #name_str))?
							.clone();
						let try_from = <#try_from as tangram_database::value::Deserialize>::deserialize(value)
							.map_err(|error| format!("failed to deserialize column \"{}\": {error}", #name_str))?;
						try_from.try_into()
							.map_err(|error| format!("failed to convert column \"{}\": {error}", #name_str))?
					}
				}
			} else {
				quote! {
					#name: {
						let value = row.get(tangram_either::Either::Right(#name_str))
							.ok_or_else(|| format!("missing column \"{}\"", #name_str))?
							.clone();
						<#field_type as tangram_database::value::Deserialize>::deserialize(value)
							.map_err(|error| format!("failed to deserialize column \"{}\": {error}", #name_str))?
					}
				}
			}
		})
		.collect();

	let code = quote! {
		impl tangram_database::row::Deserialize for #name {
			fn deserialize(row: tangram_database::Row) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
				Ok(Self {
					#( #field_exprs, )*
				})
			}
		}
	};

	code.into()
}

#[proc_macro_derive(PostgresRowDeserialize, attributes(tangram_database))]
pub fn postgres_row_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	let name = &input.ident;

	let syn::Data::Struct(data) = &input.data else {
		return syn::Error::new_spanned(&input, "this can only be derived for structs")
			.to_compile_error()
			.into();
	};

	let syn::Fields::Named(fields) = &data.fields else {
		return syn::Error::new_spanned(
			&input,
			"this can only be derived for structs with named fields",
		)
		.to_compile_error()
		.into();
	};

	let field_exprs: Vec<_> = fields
		.named
		.iter()
		.map(|field| {
			let name = field.ident.as_ref().unwrap();
			let name_str = name.to_string();
			let field_type = &field.ty;
			let attrs = FieldAttrs::from_attrs(&field.attrs);

			if let Some(deserialize_with) = attrs.deserialize_with {
				quote! {
					#name: {
						#deserialize_with(row, #name_str)
							.map_err(|error| tangram_database::postgres::Error::other(format!("failed to deserialize column \"{}\": {error}", #name_str)))?
					}
				}
			} else if let Some(as_type) = attrs.as_type {
				quote! {
					#name: {
						let raw = row.get::<_, tangram_database::postgres::value::Raw>(#name_str);
						<#as_type as tangram_database::postgres::value::DeserializeAs<#field_type>>::deserialize_as(raw.ty(), raw.raw())
							.map_err(|error| tangram_database::postgres::Error::other(format!("failed to deserialize column \"{}\" (type {}): {error}", #name_str, raw.ty())))?
					}
				}
			} else if let Some(try_from) = attrs.try_from {
				quote! {
					#name: {
						row.get::<_, #try_from>(#name_str).try_into()
							.map_err(|error| tangram_database::postgres::Error::other(format!("failed to convert column \"{}\": {error}", #name_str)))?
					}
				}
			} else {
				quote! {
					#name: row.get::<_, #field_type>(#name_str)
				}
			}
		})
		.collect();

	let code = quote! {
		impl tangram_database::postgres::row::Deserialize for #name {
			fn deserialize(row: &tokio_postgres::Row) -> Result<Self, tangram_database::postgres::Error> {
				Ok(Self {
					#( #field_exprs, )*
				})
			}
		}
	};

	code.into()
}

#[proc_macro_derive(SqliteRowDeserialize, attributes(tangram_database))]
pub fn sqlite_row_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	let name = &input.ident;

	let syn::Data::Struct(data) = &input.data else {
		return syn::Error::new_spanned(&input, "this can only be derived for structs")
			.to_compile_error()
			.into();
	};

	let syn::Fields::Named(fields) = &data.fields else {
		return syn::Error::new_spanned(
			&input,
			"this can only be derived for structs with named fields",
		)
		.to_compile_error()
		.into();
	};

	let field_exprs: Vec<_> = fields
		.named
		.iter()
		.map(|field| {
			let name = field.ident.as_ref().unwrap();
			let name_str = name.to_string();
			let field_type = &field.ty;
			let attrs = FieldAttrs::from_attrs(&field.attrs);

			if let Some(deserialize_with) = attrs.deserialize_with {
				quote! {
					#name: {
						#deserialize_with(row, #name_str)
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to deserialize column \"{}\": {error}", #name_str)))?
					}
				}
			} else if let Some(as_type) = attrs.as_type {
				quote! {
					#name: {
						let value = row.get_ref(#name_str)
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to get column \"{}\": {error}", #name_str)))?;
						<#as_type as tangram_database::sqlite::value::DeserializeAs<#field_type>>::deserialize_as(value.into())
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to deserialize column \"{}\": {error}", #name_str)))?
					}
				}
			} else if let Some(try_from) = attrs.try_from {
				quote! {
					#name: {
						let value = row.get::<_, #try_from>(#name_str)
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to get column \"{}\": {error}", #name_str)))?;
						value.try_into()
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to convert column \"{}\": {error}", #name_str)))?
					}
				}
			} else {
				quote! {
					#name: {
						row.get(#name_str)
							.map_err(|error| tangram_database::sqlite::Error::other(format!("failed to deserialize column \"{}\": {error}", #name_str)))?
					}
				}
			}
		})
		.collect();

	let code = quote! {
		impl tangram_database::sqlite::row::Deserialize for #name {
			fn deserialize(row: &rusqlite::Row) -> Result<Self, tangram_database::sqlite::Error> {
				Ok(Self {
					#( #field_exprs, )*
				})
			}
		}
	};

	code.into()
}

impl FieldAttrs {
	fn from_attrs(attrs: &[syn::Attribute]) -> Self {
		let mut field_attrs = Self {
			try_from: None,
			deserialize_with: None,
			as_type: None,
		};

		for attr in attrs {
			if attr.path().is_ident("tangram_database") {
				// Get the tokens from the attribute list.
				let syn::Meta::List(list) = &attr.meta else {
					continue;
				};

				// Parse tokens manually to handle the `as` keyword.
				let mut tokens = list.tokens.clone().into_iter().peekable();
				while let Some(token) = tokens.next() {
					let proc_macro2::TokenTree::Ident(ident) = token else {
						continue;
					};
					let ident_str = ident.to_string();

					// Skip the `=` token.
					let Some(proc_macro2::TokenTree::Punct(punct)) = tokens.next() else {
						continue;
					};
					if punct.as_char() != '=' {
						continue;
					}

					// Get the string literal value.
					let Some(proc_macro2::TokenTree::Literal(lit)) = tokens.next() else {
						continue;
					};
					let Ok(lit_str) =
						syn::parse2::<syn::LitStr>(proc_macro2::TokenTree::Literal(lit).into())
					else {
						continue;
					};
					let value = lit_str.value();

					match ident_str.as_str() {
						"deserialize_with" => {
							field_attrs.deserialize_with = syn::parse_str(&value).ok();
						},
						"as" => {
							field_attrs.as_type = syn::parse_str(&value).ok();
						},
						"try_from" => {
							field_attrs.try_from = syn::parse_str(&value).ok();
						},
						_ => {},
					}

					// Skip the comma if present.
					if let Some(proc_macro2::TokenTree::Punct(punct)) = tokens.peek()
						&& punct.as_char() == ','
					{
						tokens.next();
					}
				}
			}
		}

		field_attrs
	}
}
