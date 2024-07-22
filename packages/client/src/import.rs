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
	Object,
	Artifact,
	Blob,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Lock,
	Target,
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
				let attributes = serde_json::from_value::<tg::reference::Query>(attributes)
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
				let query = tg::reference::Query {
					name,
					overrides,
					path,
					remote,
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

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Kind::Js => write!(f, "js"),
			Kind::Ts => write!(f, "ts"),
			Kind::Object => write!(f, "object"),
			Kind::Artifact => write!(f, "artifact"),
			Kind::Blob => write!(f, "blob"),
			Kind::Leaf => write!(f, "leaf"),
			Kind::Branch => write!(f, "branch"),
			Kind::Directory => write!(f, "directory"),
			Kind::File => write!(f, "file"),
			Kind::Symlink => write!(f, "symlink"),
			Kind::Lock => write!(f, "lock"),
			Kind::Target => write!(f, "target"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"js" => Ok(Kind::Js),
			"ts" => Ok(Kind::Ts),
			"object" => Ok(Kind::Object),
			"artifact" => Ok(Kind::Artifact),
			"blob" => Ok(Kind::Blob),
			"leaf" => Ok(Kind::Leaf),
			"branch" => Ok(Kind::Branch),
			"directory" => Ok(Kind::Directory),
			"file" => Ok(Kind::File),
			"symlink" => Ok(Kind::Symlink),
			"lock" => Ok(Kind::Lock),
			"target" => Ok(Kind::Target),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
