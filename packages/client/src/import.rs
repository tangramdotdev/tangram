use crate as tg;
use std::collections::BTreeMap;

/// An import in a module.
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
				let name = reference
					.query()
					.and_then(|query| query.name.clone())
					.or(attributes.name);
				let overrides = reference
					.query()
					.and_then(|query| query.overrides.clone())
					.or(attributes.overrides);
				let path = reference
					.query()
					.and_then(|query| query.path.clone())
					.or(attributes.path);
				let remote = reference
					.query()
					.and_then(|query| query.remote.clone())
					.or(attributes.remote);
				let subpath = reference
					.query()
					.and_then(|query| query.subpath.clone())
					.or(attributes.subpath);
				let query = tg::reference::Options {
					name,
					overrides,
					path,
					remote,
					subpath,
				};
				let query = serde_urlencoded::to_string(query)
					.map_err(|source| tg::error!(!source, "failed to serialize the query"))?;
				let uri = reference.uri().to_builder().query(query).build().unwrap();
				tg::Reference::with_uri(uri)?
			}
		} else {
			reference
		};

		Ok(Import { kind, reference })
	}
}
