use super::{Enum, StructVariantField, Variant, VariantKind};

impl<'a> Enum<'a> {
	pub fn parse(input: &'a syn::DeriveInput, data: &'a syn::DataEnum) -> syn::Result<Enum<'a>> {
		// Initialize the attrs.
		let mut display = false;
		let mut from_str = false;
		let mut into = None;
		let mut try_from = None;

		// Get the tangram_serialize attr.
		let attr = input
			.attrs
			.iter()
			.find(|attr| attr.path.is_ident("tangram_serialize"));

		if let Some(attr) = attr {
			// Parse the tangram_serialize attr as a list.
			let meta = attr.parse_meta()?;
			let syn::Meta::List(list) = meta else {
				return Err(syn::Error::new_spanned(
					attr,
					"The tangram_serialize attribute must contain a list.",
				));
			};

			// Parse the list items.
			for item in &list.nested {
				match item {
					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("into") =>
					{
						// Get the value as a string literal.
						let syn::Lit::Str(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "into" must be a string."#,
							));
						};

						// Parse the value as a type.
						let value = value.parse()?;

						into = Some(value);
					},

					// Handle the "display" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("display") => {
						display = true;
					},

					// Handle the "from_str" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("from_str") => {
						from_str = true;
					},

					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("try_from") =>
					{
						// Get the value as a string literal.
						let syn::Lit::Str(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "try_from" must be a string."#,
							));
						};

						// Parse the value as a type.
						let value = value.parse()?;

						try_from = Some(value);
					},

					_ => {},
				}
			}
		}

		// Validate attribute combinations
		if display && into.is_some() {
			return Err(syn::Error::new_spanned(
				input,
				"display attribute cannot be used together with into attribute",
			));
		}

		if display && try_from.is_some() {
			return Err(syn::Error::new_spanned(
				input,
				"display attribute cannot be used together with try_from attribute",
			));
		}

		if from_str && try_from.is_some() {
			return Err(syn::Error::new_spanned(
				input,
				"from_str attribute cannot be used together with try_from attribute",
			));
		}

		// Parse the variants.
		let mut variants = data
			.variants
			.iter()
			.map(Variant::parse)
			.collect::<syn::Result<Vec<_>>>()?;

		// Sort the variants by ID.
		variants.sort_by_key(|field| field.id);

		// Create the enum.
		let enum_ = Enum {
			display,
			from_str,
			ident: &input.ident,
			generics: &input.generics,
			into,
			try_from,
			variants,
		};

		Ok(enum_)
	}
}

impl<'a> Variant<'a> {
	pub fn parse(variant: &'a syn::Variant) -> syn::Result<Variant<'a>> {
		// Initialize the attrs.
		let mut id = None;

		// Get the ident.
		let ident = &variant.ident;

		// Determine the variant kind based on the fields.
		let variant_kind = match &variant.fields {
			syn::Fields::Unit => VariantKind::Unit,
			syn::Fields::Unnamed(fields) => {
				let types = fields.unnamed.iter().map(|f| &f.ty).collect();
				VariantKind::Tuple(types)
			},
			syn::Fields::Named(fields) => {
				let struct_fields = fields
					.named
					.iter()
					.map(|f| {
						let field_ident = f.ident.as_ref().ok_or_else(|| {
							syn::Error::new_spanned(f, "Struct field must have a name")
						})?;
						Ok(StructVariantField {
							ident: field_ident,
							ty: &f.ty,
						})
					})
					.collect::<syn::Result<Vec<_>>>()?;
				VariantKind::Struct(struct_fields)
			},
		};

		// Get the tangram_serialize attr.
		let attr = variant
			.attrs
			.iter()
			.find(|attr| attr.path.is_ident("tangram_serialize"));

		if let Some(attr) = attr {
			// Parse the tangram_serialize attr as a list.
			let meta = attr.parse_meta()?;
			let syn::Meta::List(list) = meta else {
				return Err(syn::Error::new_spanned(
					attr,
					"The tangram_serialize attribute must contain a list.",
				));
			};

			// Parse the list items.
			for item in &list.nested {
				match item {
					// Handle the "id" key.
					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("id") =>
					{
						// Get the value as an integer literal.
						let syn::Lit::Int(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "id" must be an integer."#,
							));
						};

						// Parse the value as an integer.
						let value = value.base10_parse().map_err(|_| {
							syn::Error::new_spanned(
								item,
								r#"The value for the attribute "id" must be an integer."#,
							)
						})?;

						id = Some(value);
					},

					_ => {},
				}
			}
		}

		Ok(Variant {
			id,
			ident,
			kind: variant_kind,
		})
	}
}
