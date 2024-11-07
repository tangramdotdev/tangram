use futures::{stream::FuturesUnordered, TryStreamExt as _};
use std::{
	borrow::Cow,
	collections::BTreeMap,
	os::unix::{ffi::OsStrExt, fs::PermissionsExt as _},
	path::Path,
};
use tokio::io::AsyncWriteExt as _;

pub enum Artifact {
	Directory {
		entries: BTreeMap<Cow<'static, str>, Self>,
	},
	File {
		contents: Cow<'static, str>,
		executable: bool,
	},
	Symlink {
		target: Cow<'static, str>,
	},
}

impl Artifact {
	pub async fn to_path(&self, path: &Path) -> std::io::Result<()> {
		match self {
			Self::Directory { entries } => {
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
			Self::File {
				contents,
				executable,
			} => {
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
			Self::Symlink { target } => {
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
			Self::Directory { entries } => {
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

			Self::File {
				contents,
				executable,
			} => {
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

			Self::Symlink { target } => {
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

impl From<&'static str> for Artifact {
	fn from(value: &'static str) -> Self {
		Self::File {
			contents: value.into(),
			executable: false,
		}
	}
}

impl From<String> for Artifact {
	fn from(value: String) -> Self {
		Self::File {
			contents: value.into(),
			executable: false,
		}
	}
}

#[macro_export]
macro_rules! directory {
	{ $($name:expr => $artifact:expr),* $(,)? } => {{
		let mut entries = ::std::collections::BTreeMap::new();
		$(
			entries.insert($name.into(), $artifact.into());
		)*
		$crate::artifact::Artifact::Directory { entries }
	}};
}

#[macro_export]
macro_rules! file {
	($contents:expr, $executable:expr) => {{
		$crate::artifact::Artifact::File {
			contents: $contents.into(),
			executable: $executable,
		}
	}};
}

#[macro_export]
macro_rules! symlink {
	($target:expr) => {{
		$crate::artifact::Artifact::Symlink {
			target: $target.into(),
		}
	}};
}
