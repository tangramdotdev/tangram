use futures::{stream::FuturesUnordered, TryStreamExt as _};
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
			Ok(Self::Directory(Directory { entries }))
		} else if metadata.is_file() {
			let contents =
				Cow::Owned(tokio::fs::read_to_string(path).await.map_err(|error| {
					tg::error!(source = error, "could not read file to string")
				})?);
			let executable = metadata.permissions().mode() & 0o111 != 0;
			Ok(Self::File(File {
				contents,
				executable,
			}))
		} else if metadata.is_symlink() {
			let target = Cow::Owned(
				tokio::fs::read_link(path)
					.await
					.unwrap()
					.to_str()
					.unwrap()
					.to_owned(),
			);
			Ok(Self::Symlink(Symlink { target }))
		} else {
			Err(tg::error!(?path, "expected a file, directory, or symlink"))
		}
	}

	pub async fn to_path(&self, path: &Path) -> std::io::Result<()> {
		match self {
			Self::Directory(Directory { entries }) => {
				tokio::fs::create_dir(&path).await?;
				entries
					.iter()
					.map(|(name, artifact)| async {
						let path = path.join(name.as_ref());
						artifact.to_path(&path).await?;
						Ok::<_, std::io::Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
			},
			Self::File(File {
				contents,
				executable,
			}) => {
				let mut file = tokio::fs::OpenOptions::new()
					.write(true)
					.create(true)
					.truncate(true)
					.open(&path)
					.await?;
				file.write_all(contents.as_bytes()).await?;
				if *executable {
					file.set_permissions(std::fs::Permissions::from_mode(0o755))
						.await?;
				}
			},
			Self::Symlink(Symlink { target }) => {
				tokio::fs::symlink(target.as_ref(), path).await?;
			},
		}
		Ok(())
	}

	pub async fn matches(&self, path: &Path) -> std::io::Result<bool> {
		let metadata = match tokio::fs::symlink_metadata(&path).await {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(false);
			},
			Err(error) => {
				return Err(error);
			},
		};

		match self {
			Self::Directory(Directory { entries }) => {
				if !metadata.is_dir() {
					return Ok(false);
				}
				let mut names_ = Vec::new();
				let mut read_dir = tokio::fs::read_dir(&path).await?;
				while let Some(entry) = read_dir.next_entry().await? {
					names_.push(entry.file_name());
				}
				names_.sort_unstable();
				let names = entries
					.keys()
					.map(|name| std::ffi::OsStr::new(name.as_ref()));
				if !names.eq(names_) {
					return Ok(false);
				}
				for (name, artifact) in entries {
					let path = path.join(name.as_ref());
					if !Box::pin(artifact.matches(&path)).await? {
						return Ok(false);
					}
				}
			},

			Self::File(File {
				contents,
				executable,
			}) => {
				if !metadata.is_file() {
					return Ok(false);
				}
				let executable_ = metadata.permissions().mode() & 0o111 != 0;
				if *executable != executable_ {
					return Ok(false);
				}
				let contents_ = tokio::fs::read_to_string(&path).await?;
				if contents.as_ref() != contents_ {
					return Ok(false);
				}
			},

			Self::Symlink(Symlink { target }) => {
				if !metadata.is_symlink() {
					return Ok(false);
				}
				let target_ = tokio::fs::read_link(&path).await?;
				if target.as_ref().as_bytes() != target_.as_os_str().as_bytes() {
					return Ok(false);
				}
			},
		}

		Ok(true)
	}
}

impl Directory {
	#[must_use]
	pub fn with_entries(entries: BTreeMap<Cow<'static, str>, Artifact>) -> Self {
		Self { entries }
	}
}

impl From<&'static str> for Artifact {
	fn from(value: &'static str) -> Self {
		Self::File(File {
			contents: value.into(),
			executable: false,
		})
	}
}

impl From<String> for Artifact {
	fn from(value: String) -> Self {
		Self::File(File {
			contents: value.into(),
			executable: false,
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
		$crate::file!(@executable $($($arg)*)?);
		$crate::artifact::File { contents, executable }
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
