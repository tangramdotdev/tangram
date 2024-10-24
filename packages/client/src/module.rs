use crate as tg;
use std::path::{Path, PathBuf};
use tangram_either::Either;

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
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Item {
	Path(PathBuf),
	Object(tg::object::Id),
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
		// let Some(name) = tg::package::try_get_root_module_file_name(
		// 	handle,
		// 	package.as_ref().map_right(AsRef::as_ref),
		// )
		// .await?
		// else {
		// 	return Ok(None);
		// };
		// let kind = if name.ends_with("js") {
		// 	tg::module::Kind::Js
		// } else if name.ends_with("ts") {
		// 	tg::module::Kind::Ts
		// } else {
		// 	unreachable!()
		// };
		// let item = match package {
		// 	Either::Left(object) => Either::Left(object.id(handle).await?),
		// 	Either::Right(path) => Either::Right(path),
		// };
		// let subpath = name.into();
		// let referent = tg::Referent {
		// 	object: Some(item),
		// 	subpath: Some(path),

		// };
		// let module = Self {
		// 	kind,
		// 	referent,
		// };
		// Ok(Some(module))
		todo!()
	}

	pub async fn with_path(path: impl AsRef<Path>) -> tg::Result<Self> {
		// let path = path.as_ref();
		// #[allow(clippy::case_sensitive_file_extension_comparisons)]
		// let kind = if path.extension().is_some_and(|extension| extension == "js") {
		// 	tg::module::Kind::Js
		// } else if path.extension().is_some_and(|extension| extension == "ts") {
		// 	tg::module::Kind::Ts
		// } else {
		// 	let metadata = tokio::fs::symlink_metadata(path)
		// 		.await
		// 		.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		// 	if metadata.is_dir() {
		// 		tg::module::Kind::Directory
		// 	} else if metadata.is_file() {
		// 		tg::module::Kind::File
		// 	} else if metadata.is_symlink() {
		// 		tg::module::Kind::Symlink
		// 	} else {
		// 		return Err(tg::error!("expected a directory, file, or symlink"));
		// 	}
		// };
		// let package = tg::package::try_get_nearest_package_path_for_path(path).await?;
		// let subpath = if let Some(package) = &package {
		// 	path.strip_prefix(package).unwrap()
		// } else {
		// 	path
		// };
		// let package = package.map(|package| Either::Right(package.to_owned()));
		// let referent = tg::Referent {
		// 	item: package,
		// 	subpath: Some(subpath.to_owned()),
		// 	tag: None,
		// };
		// let module = Self { kind, referent };
		// Ok(module)
		todo!()
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
			Ok(Self::Path(s.into()))
		} else {
			Ok(Self::Object(s.parse()?))
		}
	}
}
