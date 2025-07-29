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
			.find(|attr| attr.path().is_ident("tangram_serialize"));

		if let Some(attr) = attr {
			attr.parse_nested_meta(|meta| {
				if meta.path.is_ident("display") {
					display = true;
					Ok(())
				} else if meta.path.is_ident("from_str") {
					from_str = true;
					Ok(())
				} else if meta.path.is_ident("into") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					into = Some(lit.parse()?);
					Ok(())
				} else if meta.path.is_ident("transparent") {
					transparent = true;
					Ok(())
				} else if meta.path.is_ident("try_from") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					try_from = Some(lit.parse()?);
					Ok(())
				} else {
					Err(meta.error("unsupported attribute"))
				}
			})?;
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
			.find(|attr| attr.path().is_ident("tangram_serialize"));

		// Parse the list items.
		if let Some(attr) = attr {
			attr.parse_nested_meta(|meta| {
				if meta.path.is_ident("deserialize_with") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					deserialize_with = Some(lit.value());
					Ok(())
				} else if meta.path.is_ident("id") {
					let value = meta.value()?;
					let lit: syn::LitInt = value.parse()?;
					id = Some(lit.base10_parse()?);
					Ok(())
				} else if meta.path.is_ident("serialize_with") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					serialize_with = Some(lit.value());
					Ok(())
				} else if meta.path.is_ident("skip_serializing_if") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					skip_serializing_if = Some(lit.value());
					Ok(())
				} else if meta.path.is_ident("display") {
					display = true;
					Ok(())
				} else if meta.path.is_ident("from_str") {
					from_str = true;
					Ok(())
				} else if meta.path.is_ident("skip") {
					skip = true;
					Ok(())
				} else if meta.path.is_ident("default") {
					default = true;
					Ok(())
				} else {
					Err(meta.error("unsupported attribute"))
				}
			})?;
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
