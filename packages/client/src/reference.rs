use crate::{self as tg, handle::Ext as _};
use either::Either;
use std::collections::BTreeMap;
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
	pub kind: Option<Kind>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overrides: Option<BTreeMap<String, tg::Reference>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
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
	Build,
	Object,
	Artifact,
	Blob,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Package,
	Target,
}

impl Reference {
	pub fn with_uri(uri: uri::Reference) -> tg::Result<Self> {
		let path = uri.path();
		let path = if let Ok(build) = path.parse::<tg::build::Id>() {
			Path::Build(build)
		} else if let Ok(build) = path.parse::<tg::object::Id>() {
			Path::Object(build)
		} else if path.starts_with('.') || path.starts_with('/') {
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid path"))?;
			Path::Path(path)
		} else {
			let tag = path.parse()?;
			Path::Tag(tag)
		};
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
		Self::with_uri(path.to_string().parse().unwrap()).unwrap()
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

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Kind::Build => write!(f, "build"),
			Kind::Object => write!(f, "object"),
			Kind::Artifact => write!(f, "artifact"),
			Kind::Blob => write!(f, "blob"),
			Kind::Leaf => write!(f, "leaf"),
			Kind::Branch => write!(f, "branch"),
			Kind::Directory => write!(f, "directory"),
			Kind::File => write!(f, "file"),
			Kind::Symlink => write!(f, "symlink"),
			Kind::Package => write!(f, "package"),
			Kind::Target => write!(f, "target"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"build" => Ok(Kind::Build),
			"object" => Ok(Kind::Object),
			"artifact" => Ok(Kind::Artifact),
			"blob" => Ok(Kind::Blob),
			"leaf" => Ok(Kind::Leaf),
			"branch" => Ok(Kind::Branch),
			"directory" => Ok(Kind::Directory),
			"file" => Ok(Kind::File),
			"symlink" => Ok(Kind::Symlink),
			"package" => Ok(Kind::Package),
			"target" => Ok(Kind::Target),
			_ => Err(tg::error!("invalid kind")),
		}
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
