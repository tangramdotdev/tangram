use crate::{self as tg, handle::Ext as _};
use std::collections::BTreeMap;
use tangram_either::Either;
use tangram_uri as uri;

pub mod get;

#[derive(Clone, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub struct Reference {
	uri: uri::Reference,
	path: Path,
	query: Option<Query>,
}

#[derive(
	Clone,
	Debug,
	derive_more::Display,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Path {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Path(tg::Path),
	Tag(tg::tag::Pattern),
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Query {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub follow: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overrides: Option<BTreeMap<String, tg::Reference>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl Reference {
	pub fn with_uri(uri: uri::Reference) -> tg::Result<Self> {
		let path = uri.path().parse()?;
		let query = uri
			.query()
			.map(|query| {
				serde_urlencoded::from_str(query)
					.map_err(|source| tg::error!(!source, "invalid query"))
			})
			.transpose()?;
		Ok(Self { uri, path, query })
	}

	pub fn with_path_and_query(path: &Path, query: Option<&Query>) -> Self {
		let path = path.to_string();
		let query = query
			.as_ref()
			.map(serde_urlencoded::to_string)
			.transpose()
			.unwrap();
		let uri = uri::Reference::builder()
			.path(path)
			.query(query)
			.build()
			.unwrap();
		Self::with_uri(uri).unwrap()
	}

	#[must_use]
	pub fn with_build(build: &tg::build::Id) -> Self {
		Self::with_uri(build.to_string().parse().unwrap()).unwrap()
	}

	#[must_use]
	pub fn with_object(object: &tg::object::Id) -> Self {
		Self::with_uri(object.to_string().parse().unwrap()).unwrap()
	}

	#[must_use]
	pub fn with_path(path: &tg::Path) -> Self {
		let components = path
			.components()
			.iter()
			.map(|component| match component {
				tg::path::Component::Normal(string) => {
					tg::path::Component::Normal(urlencoding::encode(string).as_ref().to_owned())
				},
				component => component.clone(),
			})
			.collect::<Vec<_>>();
		let string = tg::Path::with_components(components).to_string();
		Self::with_uri(string.parse().unwrap()).unwrap()
	}

	pub fn with_tag(tag: &tg::tag::Pattern) -> tg::Result<Self> {
		let uri = tag
			.to_string()
			.parse()
			.map_err(|source| tg::error!(!source, "invalid tag"))?;
		let reference = Self::with_uri(uri).unwrap();
		Ok(reference)
	}

	#[must_use]
	pub fn uri(&self) -> &uri::Reference {
		&self.uri
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.uri.as_str()
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	#[must_use]
	pub fn query(&self) -> Option<&Query> {
		self.query.as_ref()
	}

	pub async fn get<H>(&self, handle: &H) -> tg::Result<Either<tg::Build, tg::Object>>
	where
		H: tg::Handle,
	{
		handle.get_reference(self).await.map(|output| {
			output
				.item
				.map_left(tg::Build::with_id)
				.map_right(tg::Object::with_id)
		})
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.uri)
	}
}

impl std::str::FromStr for Reference {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri =
			uri::Reference::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let reference = Self::with_uri(uri)?;
		Ok(reference)
	}
}

impl std::str::FromStr for Path {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(build) = s.parse() {
			return Ok(Self::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(Self::Object(object));
		}
		if s.starts_with('.') || s.starts_with('/') {
			let path = s.parse()?;
			return Ok(Self::Path(path));
		}
		if let Ok(tag) = s.parse() {
			return Ok(Self::Tag(tag));
		}
		Err(tg::error!("invalid path"))
	}
}

impl std::cmp::Eq for Reference {}

impl std::hash::Hash for Reference {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.uri.hash(state);
	}
}

impl std::cmp::Ord for Reference {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.uri.cmp(&other.uri)
	}
}

impl std::cmp::PartialEq for Reference {
	fn eq(&self, other: &Self) -> bool {
		self.uri.eq(&other.uri)
	}
}

impl std::cmp::PartialOrd for Reference {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
