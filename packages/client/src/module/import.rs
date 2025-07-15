use crate as tg;
use std::collections::BTreeMap;

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
				let id = reference.options().id.clone().or(attributes.id);
				let path = reference.options().path.clone().or(attributes.path);
				let remote = reference.options().remote.clone().or(attributes.remote);
				let tag = reference.options().tag.clone().or(attributes.tag);
				let query = tg::reference::Options {
					id,
					path,
					remote,
					tag,
				};
				let query = serde_urlencoded::to_string(query)
					.map_err(|source| tg::error!(!source, "failed to serialize the query"))?;
				let uri = reference.uri().to_builder().query(query).build().unwrap();
				tg::Reference::with_uri(uri)?
			}
		} else {
			reference
		};

		let import = Import { kind, reference };

		Ok(import)
	}
}
