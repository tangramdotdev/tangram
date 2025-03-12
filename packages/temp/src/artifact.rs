use futures::{StreamExt as _, TryStreamExt as _, stream};
use std::{
	borrow::Cow,
	collections::BTreeMap,
	os::unix::{ffi::OsStrExt, fs::PermissionsExt as _},
	path::Path,
};
use tangram_client as tg;
use tokio::io::AsyncWriteExt as _;

#[derive(
	Clone,
	Debug,
	PartialEq,
	PartialOrd,
	Eq,
	Ord,
	derive_more::From,
	derive_more::IsVariant,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Artifact {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, serde::Deserialize, serde::Serialize)]
pub struct Directory {
	pub entries: BTreeMap<Cow<'static, str>, Artifact>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, serde::Deserialize, serde::Serialize)]
pub struct File {
	pub contents: Cow<'static, str>,
	pub executable: bool,
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub xattrs: BTreeMap<Cow<'static, str>, Cow<'static, str>>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, serde::Deserialize, serde::Serialize)]
pub struct Symlink {
	pub target: Cow<'static, str>,
}

impl Artifact {
	pub async fn with_path(path: &Path) -> tg::Result<Self> {
		let metadata = tokio::fs::symlink_metadata(path)
			.await
			.map_err(|error| tg::error!(source = error, "failed to read the metadata"))?;
		if metadata.is_dir() {
			Ok(Self::Directory(
				Directory::with_path(path, Some(metadata)).await?,
			))
		} else if metadata.is_file() {
			Ok(Self::File(File::with_path(path, Some(metadata)).await?))
		} else if metadata.is_symlink() {
			Ok(Self::Symlink(
				Symlink::with_path(path, Some(metadata)).await?,
			))
		} else {
			Err(tg::error!(?path, "expected a directory, file, or symlink"))
		}
	}

	pub async fn to_path(&self, path: &Path) -> tg::Result<()> {
		match self {
			Self::Directory(directory) => {
				directory.to_path(path).await?;
			},
			Self::File(file) => {
				file.to_path(path).await?;
			},
			Self::Symlink(symlink) => {
				symlink.to_path(path).await?;
			},
		}
		Ok(())
	}

	pub async fn matches(&self, path: &Path) -> tg::Result<bool> {
		let metadata = match tokio::fs::symlink_metadata(&path).await {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(false);
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the metadata"));
			},
		};

		match self {
			Self::Directory(directory) => directory.matches(path, Some(metadata)).await,
			Self::File(file) => file.matches(path, Some(metadata)).await,
			Self::Symlink(symlink) => symlink.matches(path, Some(metadata)).await,
		}
	}
}

impl Directory {
	#[must_use]
	pub fn with_entries(entries: BTreeMap<Cow<'static, str>, Artifact>) -> Self {
		Self { entries }
	}

	pub async fn with_path(path: &Path, _metadata: Option<std::fs::Metadata>) -> tg::Result<Self> {
		let mut entries = BTreeMap::new();
		let mut read_dir = tokio::fs::read_dir(path)
			.await
			.map_err(|error| tg::error!(source = error, "could not read the directory"))?;
		while let Some(entry) = read_dir
			.next_entry()
			.await
			.map_err(|error| tg::error!(source = error, "could not read the directory entry"))?
		{
			let name = entry
				.file_name()
				.into_string()
				.map_err(|_| tg::error!("could not convert the file name to a string"))?
				.into();
			let artifact = Box::pin(Artifact::with_path(entry.path().as_path())).await?;
			entries.insert(name, artifact);
		}
		Ok(Self { entries })
	}

	pub async fn to_path(&self, path: &Path) -> tg::Result<()> {
		tokio::fs::create_dir(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		stream::iter(&self.entries)
			.then(|(name, artifact)| async {
				let path = path.join(name.as_ref());
				Box::pin(artifact.to_path(&path)).await?;
				tg::ok(())
			})
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	pub async fn matches(
		&self,
		path: &Path,
		metadata: Option<std::fs::Metadata>,
	) -> tg::Result<bool> {
		let metadata = if let Some(metadata) = metadata {
			metadata
		} else {
			tokio::fs::symlink_metadata(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the metadata"))?
		};
		if !metadata.is_dir() {
			return Ok(false);
		}
		let mut names_ = Vec::new();
		let mut read_dir = tokio::fs::read_dir(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the directory"))?;
		while let Some(entry) = read_dir
			.next_entry()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the entry"))?
		{
			names_.push(entry.file_name());
		}
		names_.sort_unstable();
		let names = self
			.entries
			.keys()
			.map(|name| std::ffi::OsStr::new(name.as_ref()));
		if !names.eq(names_) {
			return Ok(false);
		}
		for (name, artifact) in &self.entries {
			let path = path.join(name.as_ref());
			if !Box::pin(artifact.matches(&path)).await? {
				return Ok(false);
			}
		}
		Ok(true)
	}
}

impl File {
	#[must_use]
	pub fn with_xattr(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
		let name = Cow::Owned(name.as_ref().to_owned());
		let value = Cow::Owned(value.as_ref().to_owned());
		self.xattrs.insert(name, value);
		self
	}

	pub async fn with_path(path: &Path, metadata: Option<std::fs::Metadata>) -> tg::Result<Self> {
		let metadata = if let Some(metadata) = metadata {
			metadata
		} else {
			tokio::fs::symlink_metadata(path)
				.await
				.map_err(|error| tg::error!(source = error, "failed to read the metadata"))?
		};
		let contents = tokio::fs::read_to_string(path)
			.await
			.map_err(|error| tg::error!(source = error, "could not read file to string"))?
			.into();
		let executable = metadata.permissions().mode() & 0o111 != 0;
		let xattrs = xattr::list(path)
			.map_err(|source| tg::error!(!source, "could not list xattrs"))?
			.map(|name| {
				let name = name
					.to_str()
					.ok_or_else(|| tg::error!("non utf8 xattr name"))?
					.to_owned();
				if !name.starts_with("user.tangram") {
					return Ok(None);
				}
				let value = xattr::get(path, &name)
					.map_err(|source| tg::error!(!source, %name, "failed to read xattr"))?
					.ok_or_else(|| tg::error!(%name, "expected an xattr"))?;
				let value =
					String::from_utf8(value).map_err(|_| tg::error!("non utf8 xattr value"))?;
				Ok::<_, tg::Error>(Some((Cow::Owned(name), Cow::Owned(value))))
			})
			.collect::<Result<Vec<_>, _>>()?
			.into_iter()
			.flatten()
			.collect();
		Ok(Self {
			contents,
			executable,
			xattrs,
		})
	}

	pub async fn to_path(&self, path: &Path) -> tg::Result<()> {
		let mut file = tokio::fs::OpenOptions::new()
			.write(true)
			.create(true)
			.truncate(true)
			.open(path)
			.await
			.map_err(|error| tg::error!(source = error, "failed to create the file"))?;
		file.write_all(self.contents.as_bytes())
			.await
			.map_err(|error| tg::error!(source = error, "failed to write the file contents"))?;
		if self.executable {
			file.set_permissions(std::fs::Permissions::from_mode(0o755))
				.await
				.map_err(|error| {
					tg::error!(source = error, "failed to set the file permissions")
				})?;
		}
		for (name, value) in &self.xattrs {
			xattr::set(path, name.as_ref(), value.as_bytes())
				.map_err(|source| tg::error!(!source, "failed to set the extended attribute"))?;
		}
		Ok(())
	}

	pub async fn matches(
		&self,
		path: &Path,
		metadata: Option<std::fs::Metadata>,
	) -> tg::Result<bool> {
		let metadata = if let Some(metadata) = metadata {
			metadata
		} else {
			tokio::fs::symlink_metadata(path)
				.await
				.map_err(|error| tg::error!(source = error, "failed to read the metadata"))?
		};
		if !metadata.is_file() {
			return Ok(false);
		}
		let executable_ = metadata.permissions().mode() & 0o111 != 0;
		if self.executable != executable_ {
			return Ok(false);
		}
		let contents_ = tokio::fs::read_to_string(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the file"))?;
		if self.contents.as_ref() != contents_ {
			return Ok(false);
		}
		for name in
			xattr::list(path).map_err(|source| tg::error!(!source, "failed to list the xattrs"))?
		{
			let Some(name) = name.to_str() else {
				return Ok(false);
			};
			let Some(value) = xattr::get(path, name)
				.map_err(|source| tg::error!(!source, "failed to get the xattr"))?
			else {
				return Ok(false);
			};
			let Ok(value) = std::str::from_utf8(&value) else {
				return Ok(false);
			};
			if self.xattrs.get(name).map(AsRef::as_ref) != Some(value) {
				return Ok(false);
			}
		}
		Ok(true)
	}
}

impl Symlink {
	pub async fn with_path(path: &Path, _metadata: Option<std::fs::Metadata>) -> tg::Result<Self> {
		let target = tokio::fs::read_link(path)
			.await
			.unwrap()
			.to_str()
			.unwrap()
			.to_owned()
			.into();
		Ok(Self { target })
	}

	pub async fn to_path(&self, path: &Path) -> tg::Result<()> {
		tokio::fs::symlink(self.target.as_ref(), path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
		Ok(())
	}

	pub async fn matches(
		&self,
		path: &Path,
		metadata: Option<std::fs::Metadata>,
	) -> tg::Result<bool> {
		let metadata = if let Some(metadata) = metadata {
			metadata
		} else {
			tokio::fs::symlink_metadata(path)
				.await
				.map_err(|error| tg::error!(source = error, "failed to read the metadata"))?
		};
		if !metadata.is_symlink() {
			return Ok(false);
		}
		let target_ = tokio::fs::read_link(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;
		if self.target.as_ref().as_bytes() != target_.as_os_str().as_bytes() {
			return Ok(false);
		}
		Ok(true)
	}
}

impl From<&'static str> for Artifact {
	fn from(value: &'static str) -> Self {
		Self::File(File {
			contents: value.into(),
			executable: false,
			xattrs: BTreeMap::new(),
		})
	}
}

impl From<String> for Artifact {
	fn from(value: String) -> Self {
		Self::File(File {
			contents: value.into(),
			executable: false,
			xattrs: BTreeMap::new(),
		})
	}
}

#[macro_export]
macro_rules! directory {
	{ $($name:expr => $artifact:expr),* $(,)? } => {{
		let mut entries = ::std::collections::BTreeMap::new();
		$(
			entries.insert($name.into(), $artifact.into());
		)*
		$crate::artifact::Directory { entries }
	}};
}

#[macro_export]
macro_rules! file {
	(@$executable:ident executable = $value:expr $(, $($arg:tt)*)?) => {
		$executable = $value;
		$crate::file!(@$executable $($($arg)*)?);
	};
	(@$executable:ident) => {};
	($contents:expr $(, $($arg:tt)*)?) => {{
		let contents = $contents.into();
		let mut executable = false;
		let mut xattrs = std::collections::BTreeMap::new();
		$crate::file!(@executable $($($arg)*)?);
		$crate::artifact::File { contents, executable, xattrs }
	}};
}

#[macro_export]
macro_rules! symlink {
	($target:expr) => {{
		$crate::artifact::Symlink {
			target: $target.into(),
		}
	}};
}
