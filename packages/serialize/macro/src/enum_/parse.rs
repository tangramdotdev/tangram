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
			.find(|attr| attr.path().is_ident("tangram_serialize"));

		if let Some(attr) = attr {
			attr.parse_nested_meta(|meta| {
				if meta.path.is_ident("into") {
					let value = meta.value()?;
					let lit: syn::LitStr = value.parse()?;
					into = Some(lit.parse()?);
					Ok(())
				} else if meta.path.is_ident("display") {
					display = true;
					Ok(())
				} else if meta.path.is_ident("from_str") {
					from_str = true;
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
			.find(|attr| attr.path().is_ident("tangram_serialize"));

		if let Some(attr) = attr {
			attr.parse_nested_meta(|meta| {
				if meta.path.is_ident("id") {
					let value = meta.value()?;
					let lit: syn::LitInt = value.parse()?;
					id = Some(lit.base10_parse()?);
					Ok(())
				} else {
					Err(meta.error("unsupported attribute"))
				}
			})?;
		}

		Ok(Variant {
			id,
			ident,
			kind: variant_kind,
		})
	}
}
