use crate as tg;
use std::path::Path;
use url::Url;

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,
	pub object: Object,
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
pub enum Object {
	Object(tg::object::Id),
	Path(tg::Path),
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
	Artifact,
	Blob,
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Graph,
	Target,
}

impl From<Module> for Url {
	fn from(value: Module) -> Self {
		// Serialize and encode the module.
		let json = serde_json::to_string(&value).unwrap();
		let hex = data_encoding::HEXLOWER.encode(json.as_bytes());

		// Create the URL.
		format!("tg://{hex}").parse().unwrap()
	}
}

impl TryFrom<Url> for Module {
	type Error = tg::Error;

	fn try_from(url: Url) -> tg::Result<Self, Self::Error> {
		// Ensure the scheme is "tg".
		if url.scheme() != "tg" {
			return Err(tg::error!(%url, "the URL has an invalid scheme"));
		}

		// Get the domain.
		let hex = url
			.domain()
			.ok_or_else(|| tg::error!(%url, "the URL must have a domain"))?;

		// Decode.
		let json = data_encoding::HEXLOWER
			.decode(hex.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize.
		let module = serde_json::from_slice(&json)
			.map_err(|source| tg::error!(!source, "failed to deserialize the module"))?;

		Ok(module)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", Url::from(self.clone()))
	}
}

impl std::str::FromStr for Module {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let url: Url = s
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the URL"))?;
		let module = url.try_into()?;
		Ok(module)
	}
}

impl std::fmt::Display for Object {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Object(object) => write!(f, "{object}"),
			Self::Path(path) => write!(f, "{path}"),
		}
	}
}

impl std::str::FromStr for Object {
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

pub async fn get_root_module_path_for_path(path: &Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
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

pub fn is_root_module_path(path: &Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};
	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| &last == *name)
}

pub fn is_module_path(path: &Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};

	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| &last == *name)
		|| last.ends_with(".tg.js")
		|| last.ends_with(".tg.ts")
}

pub async fn try_get_lock_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	// Canonicalize the path.
	let path = tokio::fs::canonicalize(path).await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
	)?;

	// Check if this is a module path.
	if !is_module_path(path.as_ref()) {
		return Ok(None);
	}

	// Walk up to find a lockfile.
	let mut path_: &Path = path.as_ref();
	while let Some(parent) = path_.parent() {
		let lock = parent.join(LOCKFILE_FILE_NAME);
		if tokio::fs::try_exists(&lock).await.map_err(
			|source| tg::error!(!source, %path = lock.display(), "failed to check if file exists"),
		)? {
			let path = path
				.try_into()
				.map_err(|source| tg::error!(!source, "invalid path"))?;
			return Ok(Some(path));
		}
		path_ = parent;
	}

	// If no existing lockfile was found, compute the path to the new lockfile.
	let path = path
		.parent()
		.unwrap()
		.join(LOCKFILE_FILE_NAME)
		.try_into()
		.map_err(|source| tg::error!(!source, "invalid path"))?;
	Ok(Some(path))
}
