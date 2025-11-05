use {crate::prelude::*, std::collections::BTreeMap};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Import {
	pub kind: Option<tg::module::Kind>,
	pub reference: tg::Reference,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		mut attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier as a reference.
		let reference = specifier.parse::<tg::Reference>()?;

		// Parse the kind.
		let kind = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("type").or(attributes.remove("kind")))
			.map(|kind| kind.parse())
			.transpose()?;

		// Parse the remaining attributes as the query component of a reference and update the reference.
		let reference = if let Some(attributes) = attributes {
			if attributes.is_empty() {
				reference
			} else {
				let attributes = serde_json::Value::Object(
					attributes
						.into_iter()
						.map(|(key, value)| (key, serde_json::Value::String(value)))
						.collect(),
				);
				let attributes = serde_json::from_value::<tg::reference::Options>(attributes)
					.map_err(|source| tg::error!(!source, "invalid attributes"))?;
				let local = attributes.local.or(reference.options().local.clone());
				let remote = attributes.remote.or(reference.options().remote.clone());
				let options = tg::reference::Options { local, remote };
				tg::Reference::with_item_and_options(reference.item().clone(), options)
			}
		} else {
			reference
		};

		let import = Import { kind, reference };

		Ok(import)
	}
}
