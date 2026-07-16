use {
	super::Kind,
	crate::prelude::*,
	std::{collections::BTreeSet, path::PathBuf},
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Module {
	#[tangram_serialize(id = 0)]
	pub kind: Kind,

	#[tangram_serialize(id = 1)]
	pub referent: tg::Referent<Item>,
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
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Edge(tg::graph::data::Edge<tg::object::Id>),
	Path(PathBuf),
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Query {
	kind: Kind,
}

impl Module {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Item::Edge(edge) = &self.referent.item {
			edge.children(children);
		}
	}
}

impl Module {
	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self.referent.item.to_string();
		let mut builder = Uri::builder().path(&path);
		let config = serde_qs::Config::new().use_form_encoding(true);
		let mut query = config.serialize_string(&self.referent.options).unwrap();
		if !query.is_empty() {
			query.push('&');
		}
		let kind = config.serialize_string(&Query { kind: self.kind }).unwrap();
		query.push_str(&kind);
		builder = builder.query_raw(&query);
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let item = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the item"))?;
		let query = uri
			.query_raw()
			.ok_or_else(|| tg::error!("expected the query to be set"))?;
		let options = serde_qs::from_str(query)
			.map_err(|error| tg::error!(!error, "failed to deserialize the referent options"))?;
		let Query { kind } = serde_qs::from_str(query)
			.map_err(|error| tg::error!(!error, "failed to deserialize the module options"))?;
		Ok(Self {
			kind,
			referent: tg::Referent { item, options },
		})
	}

	#[must_use]
	pub fn without_token(&self) -> Self {
		let mut module = self.clone();
		module.referent = module.referent.without_token();

		module
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Module {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|error| tg::error!(!error, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Edge(edge) => {
				write!(f, "{edge}")?;
			},
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
			Ok(Self::Edge(s.parse().map_err(|error| {
				tg::error!(!error, "failed to parse the module edge")
			})?))
		}
	}
}
