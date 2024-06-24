use crate::{self as tg, handle::Ext as _};
use either::Either;
use fluent_uri::Uri;
use std::collections::BTreeMap;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub struct Reference {
	pub uri: Uri<String>,
	pub path: Path,
	pub query: Option<Query>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Path {
	Build(tg::build::Id),
	Object(tg::object::Id),
	Path(tg::Path),
	Tag(tg::tag::Pattern),
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
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
	#[must_use]
	pub fn with_build(build: tg::build::Id) -> Self {
		Self {
			uri: Uri::parse(build.to_string()).unwrap(),
			path: Path::Build(build),
			query: None,
		}
	}

	#[must_use]
	pub fn with_object(object: tg::object::Id) -> Self {
		Self {
			uri: Uri::parse(object.to_string()).unwrap(),
			path: Path::Object(object),
			query: None,
		}
	}

	#[must_use]
	pub fn with_path(path: tg::Path) -> Self {
		Self {
			uri: Uri::parse(path.to_string()).unwrap(),
			path: Path::Path(path),
			query: None,
		}
	}

	pub fn with_tag(tag: tg::tag::Pattern) -> tg::Result<Self> {
		let uri = Uri::parse(tag.as_ref().to_owned())
			.map_err(|source| tg::error!(!source, "invalid tag"))?;
		Ok(Self {
			uri,
			path: Path::Tag(tag),
			query: None,
		})
	}

	pub async fn get<H>(&self, handle: &H) -> tg::Result<Either<tg::build::Id, tg::object::Id>>
	where
		H: tg::Handle,
	{
		match &self.path {
			Path::Build(build) => Ok(Either::Left(build.clone())),
			Path::Object(object) => Ok(Either::Right(object.clone())),
			Path::Path(_) => {
				let arg = tg::package::create::Arg {
					reference: self.clone(),
					locked: false,
					remote: None,
				};
				let tg::package::create::Output { package } = handle.create_package(arg).await?;
				Ok(Either::Right(package.into()))
			},
			Path::Tag(tag) => {
				let tg::tag::get::Output { item, .. } = handle.get_tag(tag).await?;
				Ok(item)
			},
		}
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
		let string = value.to_owned();
		let uri = Uri::parse(string).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let path = uri.path().as_str();
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
				serde_urlencoded::from_str(query.as_str())
					.map_err(|source| tg::error!(!source, "invalid query"))
			})
			.transpose()?;
		Ok(Self { uri, path, query })
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
