use crate::{self as tg, handle::Ext as _};
use itertools::Itertools as _;
use std::{
	collections::BTreeMap,
	os::unix::ffi::OsStrExt as _,
	path::{Path, PathBuf},
	pin::pin,
};
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;
use tangram_uri as uri;

#[derive(Clone, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub struct Reference {
	uri: uri::Reference,
	item: Item,
	options: Option<Options>,
}

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Object(tg::object::Id),
	Path(PathBuf),
	Process(tg::process::Id),
	Tag(tg::tag::Pattern),
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overrides: Option<BTreeMap<String, tg::Reference>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subpath: Option<PathBuf>,
}

impl Reference {
	pub fn with_uri(uri: uri::Reference) -> tg::Result<Self> {
		let path = uri.path();
		let path =
			urlencoding::decode(path).map_err(|source| tg::error!(!source, "invalid path"))?;
		let item = path.parse()?;
		let options = uri
			.query()
			.map(|query| {
				serde_urlencoded::from_str(query)
					.map_err(|source| tg::error!(!source, "invalid query"))
			})
			.transpose()?;
		Ok(Self { uri, item, options })
	}

	pub fn with_item_and_options(item: &Item, options: Option<&Options>) -> Self {
		let path = item.to_string();
		let query = options
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
	pub fn with_process(process: &tg::process::Id) -> Self {
		Self::with_uri(process.to_string().parse().unwrap()).unwrap()
	}

	#[must_use]
	pub fn with_object(object: &tg::object::Id) -> Self {
		Self::with_uri(object.to_string().parse().unwrap()).unwrap()
	}

	#[must_use]
	pub fn with_path(path: impl AsRef<Path>) -> Self {
		let mut string = path
			.as_ref()
			.components()
			.map(|component| urlencoding::encode_binary(component.as_os_str().as_bytes()))
			.join("/");
		if !(string.starts_with('.') || string.starts_with('/')) {
			string.insert_str(0, "./");
		}
		let uri = string.parse().unwrap();
		Self::with_uri(uri).unwrap()
	}

	#[must_use]
	pub fn with_tag(tag: &tg::tag::Pattern) -> Self {
		let uri = urlencoding::encode(tag.as_str()).parse().unwrap();
		Self::with_uri(uri).unwrap()
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
	pub fn item(&self) -> &Item {
		&self.item
	}

	#[must_use]
	pub fn options(&self) -> Option<&Options> {
		self.options.as_ref()
	}

	pub async fn get<H>(
		&self,
		handle: &H,
	) -> tg::Result<tg::Referent<Either<tg::Process, tg::Object>>>
	where
		H: tg::Handle,
	{
		let stream = handle
			.get(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get stream"))?;
		let stream = pin!(stream);
		let output = stream.try_last().await?;
		match output {
			Some(output) => match output {
				crate::progress::Event::Output(output) => Ok(output),
				_ => Err(tg::error!("expected output")),
			},
			None => Err(tg::error!("failed to get output")),
		}
	}
}

impl Reference {
	pub fn name(&self) -> Option<&str> {
		self.options()
			.and_then(|options| options.name.as_deref())
			.or(self
				.item()
				.try_unwrap_tag_ref()
				.ok()
				.map(tg::tag::Pattern::name))
	}

	pub fn path(&self) -> Option<&Path> {
		self.options()
			.and_then(|opt| opt.path.as_ref())
			.or(self.item().try_unwrap_path_ref().ok())
			.map(PathBuf::as_ref)
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

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Item::Object(object) => {
				write!(f, "{object}")?;
			},
			Item::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Item::Process(process) => {
				write!(f, "{process}")?;
			},
			Item::Tag(tag) => {
				write!(f, "{tag}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(object) = s.parse() {
			return Ok(Self::Object(object));
		}
		if s.starts_with('.') || s.starts_with('/') {
			let path = s.strip_prefix("./").unwrap_or(s).into();
			return Ok(Self::Path(path));
		}
		if let Ok(process) = s.parse() {
			return Ok(Self::Process(process));
		}
		if let Ok(tag) = s.parse() {
			return Ok(Self::Tag(tag));
		}
		Err(tg::error!(%s, "invalid path"))
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
