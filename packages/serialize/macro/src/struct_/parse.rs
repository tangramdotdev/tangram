use super::{Field, Struct};

impl<'a> Struct<'a> {
	pub fn parse(
		input: &'a syn::DeriveInput,
		data: &'a syn::DataStruct,
	) -> syn::Result<Struct<'a>> {
		// Initialize the attrs.
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
			ident: &input.ident,
			into,
			try_from,
			fields,
		};

		Ok(struct_)
	}
}

impl<'a> Field<'a> {
	pub fn parse(field: &'a syn::Field) -> syn::Result<Field<'a>> {
		// Initialize the attrs.
		let mut deserialize_with = None;
		let mut id = None;
		let mut serialize_with = None;
		let mut skip = false;

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

					// Handle the "skip" key.
					syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("skip") => {
						skip = true;
					},

					_ => {},
				}
			}
		}

		Ok(Field {
			deserialize_with,
			id,
			ident,
			serialize_with,
			skip,
		})
	}
}
