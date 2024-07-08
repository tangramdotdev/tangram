use crate as tg;
use std::collections::BTreeMap;

/// An import in a module.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Import {
	/// The kind of the import.
	pub kind: Option<Kind>,

	/// The reference.
	pub reference: tg::Reference,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum Kind {
	Js,
	Ts,
	Artifact,
	Directory,
	File,
	Symlink,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		mut attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier as a reference.
		let reference = specifier.parse::<tg::Reference>()?;

		// Parse the type.
		let kind = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("type"));
		let kind = match kind.as_deref() {
			Some("js") => Some(Kind::Js),
			Some("ts") => Some(Kind::Ts),
			Some("artifact") => Some(Kind::Artifact),
			Some("directory") => Some(Kind::Directory),
			Some("file") => Some(Kind::File),
			Some("symlink") => Some(Kind::Symlink),
			Some(kind) => return Err(tg::error!(%kind, "unknown type")),
			None => None,
		};

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
				let attributes = serde_json::from_value::<tg::reference::Query>(attributes)
					.map_err(|source| tg::error!(!source, "invalid attributes"))?;
				let query = tg::reference::Query {
					kind: reference
						.query()
						.and_then(|query| query.kind)
						.or(attributes.kind),
					name: reference
						.query()
						.and_then(|query| query.name.clone())
						.or(attributes.name),
					overrides: reference
						.query()
						.and_then(|query| query.overrides.clone())
						.or(attributes.overrides),
					path: reference
						.query()
						.and_then(|query| query.path.clone())
						.or(attributes.path),
					remote: reference
						.query()
						.and_then(|query| query.remote.clone())
						.or(attributes.remote),
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
