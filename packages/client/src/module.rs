use crate as tg;
use std::path::PathBuf;
use url::Url;

/// A module.
#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Module {
	Js(Js),
	Ts(Js),
	Dts(Dts),
	Artifact(Artifact),
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

/// A JavaScript module.
#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Js {
	File(tg::artifact::Id),
	PackageArtifact(PackageArtifact),
	PackagePath(PackagePath),
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct PackageArtifact {
	pub artifact: tg::artifact::Id,
	pub lock: tg::lock::Id,
	pub path: tg::Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct PackagePath {
	pub package_path: PathBuf,
	pub path: tg::Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Dts {
	pub path: tg::Path,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Artifact {
	Path(tg::Path),
	Id(tg::artifact::Id),
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Directory {
	Path(tg::Path),
	Id(tg::directory::Id),
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum File {
	Path(tg::Path),
	Id(tg::file::Id),
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Symlink {
	Path(tg::Path),
	Id(tg::symlink::Id),
}

impl Module {
	pub async fn with_package_path(
		_handle: &impl tg::Handle,
		package: PathBuf,
		path: tg::Path,
	) -> tg::Result<Self> {
		let r#type = tg::import::Type::try_from_path(&path);
		let module = match r#type {
			Some(tg::import::Type::Js) => {
				let package_path = PackagePath {
					package_path: package,
					path,
				};
				Module::Js(Js::PackagePath(package_path))
			},
			Some(tg::import::Type::Ts) => {
				let package_path = PackagePath {
					package_path: package,
					path,
				};
				Module::Ts(Js::PackagePath(package_path))
			},
			Some(tg::import::Type::Dts) => {
				let dts = Dts { path };
				Module::Dts(dts)
			},
			Some(_) => return Err(tg::error!("unexpected import type")),
			None => {
				let absolute_path = package.join(&path);
				let metadata = tokio::fs::symlink_metadata(&absolute_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to read file metadata"))?;
				if metadata.is_dir() {
					Module::Directory(Directory::Path(absolute_path.try_into().unwrap()))
				} else if metadata.is_file() {
					Module::File(File::Path(absolute_path.try_into().unwrap()))
				} else if metadata.is_symlink() {
					Module::Symlink(Symlink::Path(absolute_path.try_into().unwrap()))
				} else {
					return Err(tg::error!(%path = absolute_path.display(), "invalid file type"));
				}
			},
		};
		Ok(module)
	}

	pub async fn with_package_and_lock(
		handle: &impl tg::Handle,
		package: &tg::Artifact,
		lock: &tg::Lock,
	) -> tg::Result<Self> {
		let module_path = tg::package::try_get_root_module_path(handle, package).await?;
		let (root_module, r#type) = if let Some(path) = &module_path {
			let r#type = tg::import::Type::try_from_path(path);
			let package = &package
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let root_module = package.try_get(handle, path).await?;
			(root_module, r#type)
		} else {
			(None, None)
		};
		match (r#type, root_module) {
			(Some(tg::import::Type::Js), Some(_)) => {
				let artifact = package.id(handle, None).await?;
				let lock = lock.id(handle, None).await?;
				let package_artifact = PackageArtifact {
					artifact,
					lock,
					path: module_path.unwrap(),
				};
				Ok(Module::Js(Js::PackageArtifact(package_artifact)))
			},
			(Some(tg::import::Type::Ts), Some(_)) => {
				let package_id = package.id(handle, None).await?;
				let lock_id = lock.id(handle, None).await?;
				let package_artifact = PackageArtifact {
					artifact: package_id.clone(),
					lock: lock_id,
					path: module_path.unwrap(),
				};
				Ok(Module::Ts(Js::PackageArtifact(package_artifact)))
			},
			(Some(_), _) => Err(tg::error!("unexpected import type")),
			(None, _) => match package {
				tg::Artifact::Directory(directory) => {
					let id = directory.id(handle, None).await?;
					Ok(Module::Directory(Directory::Id(id)))
				},
				tg::Artifact::File(file) => {
					let id = file.id(handle, None).await?;
					Ok(Module::File(File::Id(id)))
				},
				tg::Artifact::Symlink(symlink) => {
					let id = symlink.id(handle, None).await?;
					Ok(Module::Symlink(Symlink::Id(id)))
				},
			},
		}
	}

	#[must_use]
	pub fn artifact(&self) -> Option<tg::artifact::Id> {
		match self {
			Self::Artifact(Artifact::Id(id)) => Some(id.clone()),
			Self::Directory(Directory::Id(id)) => Some(id.clone().into()),
			Self::File(File::Id(id)) => Some(id.clone().into()),
			Self::Symlink(Symlink::Id(id)) => Some(id.clone().into()),
			Self::Js(Js::PackageArtifact(module)) | Self::Ts(Js::PackageArtifact(module)) => {
				Some(module.artifact.clone())
			},
			Self::Js(Js::File(module)) | Self::Ts(Js::File(module)) => Some(module.clone()),
			_ => None,
		}
	}

	#[must_use]
	pub fn path(&self) -> Option<tg::Path> {
		match self {
			Self::Dts(dts) => Some(dts.path.clone()),
			Self::Js(Js::PackageArtifact(js)) | Self::Ts(Js::PackageArtifact(js)) => {
				Some(js.path.clone())
			},
			Self::Js(Js::PackagePath(js)) | Self::Ts(Js::PackagePath(js)) => {
				let package_path: tg::Path = js.package_path.clone().try_into().ok()?;
				Some(package_path.join(js.path.clone()))
			},
			Self::Artifact(Artifact::Path(path))
			| Self::Directory(Directory::Path(path))
			| Self::File(File::Path(path))
			| Self::Symlink(Symlink::Path(path)) => Some(path.clone()),
			_ => None,
		}
	}
}

impl From<Module> for Url {
	fn from(value: Module) -> Self {
		// Serialize and encode the module.
		let data = serde_json::to_string(&value).unwrap();
		let data = data_encoding::HEXLOWER.encode(data.as_bytes());

		// Create the URL.
		if let Some(path) = value.path() {
			format!("tg://{data}/{path}").parse().unwrap()
		} else {
			format!("tg://{data}").parse().unwrap()
		}
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
		let data = url
			.domain()
			.ok_or_else(|| tg::error!(%url, "the URL must have a domain"))?;

		// Decode the data.
		let data = data_encoding::HEXLOWER
			.decode(data.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		// Deserialize the data.
		let module = serde_json::from_slice(&data)
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

impl From<Module> for String {
	fn from(value: Module) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Module {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
