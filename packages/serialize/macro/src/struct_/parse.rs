use super::{Field, Struct};

impl<'a> Struct<'a> {
	pub fn parse(
		input: &'a syn::DeriveInput,
		data: &'a syn::DataStruct,
	) -> syn::Result<Struct<'a>> {
		// Initialize the attrs.
		let mut display = false;
		let mut from_str = false;
		let mut into = None;
		let mut transparent = false;
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
					// Handle the "display" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("display") => {
						display = true;
					},

					// Handle the "from_str" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("from_str") => {
						from_str = true;
					},

					// Handle the "into" key.
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

					// Handle the "transparent" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path))
						if path.is_ident("transparent") =>
					{
						transparent = true;
					},

					// Handle the "try_from" key.
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

		// Validate attributes.
		if transparent && into.is_some() {
			return Err(syn::Error::new_spanned(
				input,
				"transparent attribute cannot be used together with into attribute",
			));
		}
		if transparent && try_from.is_some() {
			return Err(syn::Error::new_spanned(
				input,
				"transparent attribute cannot be used together with try_from attribute",
			));
		}
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

		// Parse the fields.
		let mut fields = data
			.fields
			.iter()
			.map(Field::parse)
			.collect::<syn::Result<Vec<_>>>()?;

		// Sort the fields by ID.
		fields.sort_by_key(|field| field.id);

		// Create the struct.
		let struct_ = Struct {
			display,
			fields,
			from_str,
			ident: &input.ident,
			generics: &input.generics,
			into,
			transparent,
			try_from,
		};

		Ok(struct_)
	}
}

impl<'a> Field<'a> {
	pub fn parse(field: &'a syn::Field) -> syn::Result<Field<'a>> {
		// Initialize the attrs.
		let mut default = false;
		let mut deserialize_with = None;
		let mut display = false;
		let mut from_str = false;
		let mut id = None;
		let mut serialize_with = None;
		let mut skip = false;
		let mut skip_serializing_if = None;

		// Get the ident.
		let ident = field.ident.as_ref();

		// Get the tangram_serialize attr.
		let attr = field
			.attrs
			.iter()
			.find(|attr| attr.path.is_ident("tangram_serialize"));

		// Parse the list items.
		if let Some(attr) = attr {
			let meta = attr.parse_meta()?;
			let syn::Meta::List(list) = meta else {
				return Err(syn::Error::new_spanned(
					attr,
					"The tangram_serialize attribute must contain a list.",
				));
			};

			for item in &list.nested {
				match item {
					// Handle the "deserialize_with" key.
					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("deserialize_with") =>
					{
						// Get the value as an identifier.
						let syn::Lit::Str(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "deserialize_with" must be a string."#,
							));
						};

						deserialize_with = Some(value.value());
					},

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

					// Handle the "serialize_with" key.
					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("serialize_with") =>
					{
						// Get the value as an identifier.
						let syn::Lit::Str(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "serialize_with" must be a string."#,
							));
						};

						serialize_with = Some(value.value());
					},

					// Handle the "skip_serializing_if" key.
					syn::NestedMeta::Meta(syn::Meta::NameValue(item))
						if item.path.is_ident("skip_serializing_if") =>
					{
						// Get the value as a string.
						let syn::Lit::Str(value) = &item.lit else {
							return Err(syn::Error::new_spanned(
								item,
								r#"The value for the attribute "skip_serializing_if" must be a string."#,
							));
						};

						skip_serializing_if = Some(value.value());
					},

					// Handle the "display" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("display") => {
						display = true;
					},

					// Handle the "from_str" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("from_str") => {
						from_str = true;
					},

					// Handle the "skip" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("skip") => {
						skip = true;
					},

					// Handle the "default" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("default") => {
						default = true;
					},

					_ => {},
				}
			}
		}

		// Validate attribute combinations
		if display && serialize_with.is_some() {
			return Err(syn::Error::new_spanned(
				field,
				"display attribute cannot be used together with serialize_with attribute",
			));
		}

		if display && deserialize_with.is_some() {
			return Err(syn::Error::new_spanned(
				field,
				"display attribute cannot be used together with deserialize_with attribute",
			));
		}

		if from_str && deserialize_with.is_some() {
			return Err(syn::Error::new_spanned(
				field,
				"from_str attribute cannot be used together with deserialize_with attribute",
			));
		}

		if default && skip {
			return Err(syn::Error::new_spanned(
				field,
				"default attribute cannot be used together with skip attribute",
			));
		}

		if skip_serializing_if.is_some() && skip {
			return Err(syn::Error::new_spanned(
				field,
				"skip_serializing_if attribute cannot be used together with skip attribute",
			));
		}

		if skip_serializing_if.is_some() && serialize_with.is_some() {
			return Err(syn::Error::new_spanned(
				field,
				"skip_serializing_if attribute cannot be used together with serialize_with attribute",
			));
		}

		Ok(Field {
			deserialize_with,
			display,
			from_str,
			id,
			ident,
			serialize_with,
			skip,
			default,
			skip_serializing_if,
		})
	}
}
