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
		let mut query = Vec::new();
		if let Some(artifact) = &self.referent.options.artifact {
			let artifact = artifact.to_string();
			let artifact = tangram_uri::encode_query_value(&artifact);
			let artifact = format!("artifact={artifact}");
			query.push(artifact);
		}
		if let Some(id) = &self.referent.options.id {
			let id = id.to_string();
			let id = tangram_uri::encode_query_value(&id);
			let id = format!("id={id}");
			query.push(id);
		}
		if let Some(name) = &self.referent.options.name {
			let name = tangram_uri::encode_query_value(name);
			let name = format!("name={name}");
			query.push(name);
		}
		if let Some(path) = &self.referent.options.path {
			let path = path.to_string_lossy();
			let path = tangram_uri::encode_query_value(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if let Some(tag) = &self.referent.options.tag {
			let tag = tag.to_string();
			let tag = tangram_uri::encode_query_value(&tag);
			let tag = format!("tag={tag}");
			query.push(tag);
		}
		let kind = self.kind.to_string();
		let kind = tangram_uri::encode_query_value(&kind);
		let kind = format!("kind={kind}");
		query.push(kind);
		let query = query.join("&");
		builder = builder.query_raw(&query);
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let mut kind = None;
		let item = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the item"))?;
		let mut options = tg::referent::Options::default();
		if let Some(query) = uri.query_raw() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=') {
					let value = tangram_uri::decode_query_value(value)
						.map_err(|_| tg::error!("failed to decode the value"))?;
					match key {
						"artifact" => {
							options.artifact.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the id"))?,
							);
						},
						"id" => {
							options.id.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the id"))?,
							);
						},
						"name" => {
							options.name.replace(value.into_owned());
						},
						"path" => {
							options.path.replace(value.into_owned().into());
						},
						"tag" => {
							options.tag.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the tag"))?,
							);
						},
						"kind" => {
							kind.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the kind"))?,
							);
						},
						_ => {},
					}
				}
			}
		}
		let kind = kind.ok_or_else(|| tg::error!("expected the kind to be set"))?;
		Ok(Self {
			kind,
			referent: tg::Referent { item, options },
		})
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
		let uri = Uri::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
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
			Ok(Self::Edge(s.parse()?))
		}
	}
}
