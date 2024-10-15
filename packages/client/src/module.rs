use crate as tg;
use std::path::{Path, PathBuf};
use tangram_either::Either;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Module {
	pub kind: Kind,
	pub object: Option<Either<tg::object::Id, PathBuf>>,
	pub path: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
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

impl Module {
	pub async fn with_package<H>(
		handle: &H,
		package: Either<tg::Object, PathBuf>,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		Self::try_with_package(handle, package)
			.await?
			.ok_or_else(|| tg::error!("the package does not contain a root module"))
	}

	pub async fn try_with_package<H>(
		handle: &H,
		package: Either<tg::Object, PathBuf>,
	) -> tg::Result<Option<Self>>
	where
		H: tg::Handle,
	{
		let Some(name) = tg::package::try_get_root_module_file_name(
			handle,
			package.as_ref().map_right(AsRef::as_ref),
		)
		.await?
		else {
			return Ok(None);
		};
		let kind = if name.ends_with("js") {
			tg::module::Kind::Js
		} else if name.ends_with("ts") {
			tg::module::Kind::Ts
		} else {
			unreachable!()
		};
		let object = match package {
			Either::Left(object) => Either::Left(object.id(handle).await?),
			Either::Right(path) => Either::Right(path),
		};
		let path = name.into();
		let module = Self {
			kind,
			object: Some(object),
			path: Some(path),
			tag: None,
		};
		Ok(Some(module))
	}

	pub async fn with_path(path: impl AsRef<Path>) -> tg::Result<Self> {
		let path = path.as_ref();
		#[allow(clippy::case_sensitive_file_extension_comparisons)]
		let kind = if path.extension().is_some_and(|extension| extension == "js") {
			tg::module::Kind::Js
		} else if path.extension().is_some_and(|extension| extension == "ts") {
			tg::module::Kind::Ts
		} else {
			let metadata = tokio::fs::symlink_metadata(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			if metadata.is_dir() {
				tg::module::Kind::Directory
			} else if metadata.is_file() {
				tg::module::Kind::File
			} else if metadata.is_symlink() {
				tg::module::Kind::Symlink
			} else {
				return Err(tg::error!("expected a directory, file, or symlink"));
			}
		};
		let package = tg::package::try_get_nearest_package_path_for_path(path).await?;
		let path = if let Some(package) = &package {
			path.strip_prefix(package).unwrap()
		} else {
			path
		};
		let package = package.map(|package| Either::Right(package.to_owned()));
		let module = Self {
			kind,
			object: package,
			path: Some(path.to_owned()),
			tag: None,
		};

		Ok(module)
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
