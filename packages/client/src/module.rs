use crate as tg;
use tangram_uri as uri;

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

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
	uri: uri::Reference,
	path: Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Path {
	pub kind: Kind,
	pub source: Source,
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
	Target,
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
	derive_more::TryUnwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[try_unwrap(ref)]
pub enum Source {
	Path(tg::Path),
	Object(tg::object::Id),
}

impl Reference {
	pub fn with_kind_and_source(kind: Kind, source: impl Into<Source>) -> Self {
		let path = Path {
			kind,
			source: source.into(),
		};
		let json = serde_json::to_string(&path).unwrap();
		let hex = data_encoding::HEXLOWER.encode(json.as_bytes());
		let string = format!("tg:{hex}");
		let uri = string.parse().unwrap();
		Self { uri, path }
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
	pub fn kind(&self) -> Kind {
		self.path.kind
	}

	#[must_use]
	pub fn source(&self) -> &Source {
		&self.path.source
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.uri)
	}
}

impl std::str::FromStr for Reference {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let reference: uri::Reference = s
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the reference"))?;

		// Ensure the scheme is "tg".
		if !matches!(reference.scheme(), Some("tg")) {
			return Err(tg::error!("the URI has an invalid scheme"));
		}

		// Get the path.
		let hex = reference.path();

		// Decode.
		let json = data_encoding::HEXLOWER
			.decode(hex.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize.
		let path = serde_json::from_slice(&json)
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		Ok(Self {
			uri: reference,
			path,
		})
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
			Self::Target => write!(f, "target"),
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
			"target" => Ok(Self::Target),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}

impl std::fmt::Display for Source {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => write!(f, "{path}"),
			Self::Object(object) => write!(f, "{object}"),
		}
	}
}

impl std::str::FromStr for Source {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s.starts_with("./") || s.starts_with("../") || s.starts_with('/') {
			let path = s.parse()?;
			Ok(Self::Path(path))
		} else {
			Ok(Self::Object(s.parse()?))
		}
	}
}

pub async fn get_root_module_path<H>(handle: &H, artifact: &tg::Artifact) -> tg::Result<tg::Path>
where
	H: tg::Handle,
{
	try_get_root_module_path(handle, artifact)
		.await?
		.ok_or_else(|| tg::error!("failed to find the package's root module"))
}

pub async fn try_get_root_module_path<H>(
	handle: &H,
	artifact: &tg::Artifact,
) -> tg::Result<Option<tg::Path>>
where
	H: tg::Handle,
{
	let Ok(artifact) = artifact.try_unwrap_directory_ref() else {
		return Ok(None);
	};
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if artifact
			.try_get(handle, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &std::path::Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

pub async fn try_get_root_module_path_for_path(
	path: &std::path::Path,
) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

#[must_use]
pub fn is_root_module_path(path: &std::path::Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};
	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| last == *name)
}

#[must_use]
pub fn is_module_path(path: &std::path::Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};

	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| last == *name)
		|| last.ends_with(".tg.js")
		|| last.ends_with(".tg.ts")
}
