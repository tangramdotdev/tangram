use crate as tg;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A module.
#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,
	pub referent: tg::Referent<Item>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Kind {
	Js,
	Ts,
	Dts,
	Object,
	Blob,
	Artifact,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Graph,
	Command,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Path(PathBuf),
	Object(tg::object::Id),
}

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
					.options()
					.and_then(|query| query.name.clone())
					.or(attributes.name);
				let overrides = reference
					.options()
					.and_then(|query| query.overrides.clone())
					.or(attributes.overrides);
				let path = reference
					.options()
					.and_then(|query| query.path.clone())
					.or(attributes.path);
				let remote = reference
					.options()
					.and_then(|query| query.remote.clone())
					.or(attributes.remote);
				let subpath = reference
					.options()
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

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Js => write!(f, "js"),
			Self::Ts => write!(f, "ts"),
			Self::Dts => write!(f, "dts"),
			Self::Object => write!(f, "object"),
			Self::Artifact => write!(f, "artifact"),
			Self::Blob => write!(f, "blob"),
			Self::Leaf => write!(f, "leaf"),
			Self::Branch => write!(f, "branch"),
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
			Self::Graph => write!(f, "graph"),
			Self::Command => write!(f, "command"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"js" => Ok(Self::Js),
			"ts" => Ok(Self::Ts),
			"dts" => Ok(Self::Dts),
			"object" => Ok(Self::Object),
			"artifact" => Ok(Self::Artifact),
			"blob" => Ok(Self::Blob),
			"leaf" => Ok(Self::Leaf),
			"branch" => Ok(Self::Branch),
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			"graph" => Ok(Self::Graph),
			"command" => Ok(Self::Command),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, ":{tag}")?;
		} else if let Some(path) = &self.referent.path {
			write!(f, ":{}", path.display())?;
		} else {
			match &self.referent.item {
				Item::Path(path) => {
					write!(f, ":{}", path.display())?;
				},
				Item::Object(object) => {
					write!(f, ":{object}")?;
				},
			}
		}
		if let Some(subpath) = &self.referent.subpath {
			write!(f, ":{}", subpath.display())?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Self::Object(object) => {
				write!(f, "{object}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.starts_with('.') || s.starts_with('/') {
			Ok(Self::Path(s.strip_prefix("./").unwrap_or(s).into()))
		} else {
			Ok(Self::Object(s.parse()?))
		}
	}
}
