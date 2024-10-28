use futures::{stream::FuturesUnordered, FutureExt as _, TryStreamExt};
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
	pub async fn to_path(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
		match self {
			Self::Directory { entries } => {
				tokio::fs::create_dir(&path).await?;
				entries
					.iter()
					.map(|(name, artifact)| async {
						let path = path.as_ref().join(name.as_ref());
						artifact.to_path(&path).boxed_local().await?;
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
				tokio::fs::symlink(&path, target.as_ref()).await?;
			},
		}
		Ok(())
	}

	pub async fn matches(&self, path: impl AsRef<Path>) -> std::io::Result<bool> {
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
					let path = path.as_ref().join(name.as_ref());
					if !artifact.matches(&path).boxed_local().await? {
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
macro_rules! artifact {
	($($artifact:tt)+) => {
		$crate::internal!($($artifact)+)
	};
}

#[macro_export]
#[doc(hidden)]
macro_rules! internal {
	(@entries $entries:ident () () ()) => {};

	(@entries $entries:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
		let _ = $entries.insert(($($key)+).into(), $value);
		$crate::internal!(@entries $entries () ($($rest)*) ($($rest)*));
	};

	(@entries $entries:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
		$crate::unexpected!($unexpected);
	};

	(@entries $entries:ident [$($key:tt)+] ($value:expr)) => {
		let _ = $entries.insert(($($key)+).into(), $value);
	};

	(@entries $entries:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
		$crate::internal!(@entries $entries [$($key)+] ($crate::internal!({$($map)*})) $($rest)*);
	};

	(@entries $entries:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
		$crate::internal!(@entries $entries [$($key)+] ($crate::internal!($value)) , $($rest)*);
	};

	(@entries $entries:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
		$crate::internal!(@entries $entries [$($key)+] ($crate::internal!($value)));
	};

	(@entries $entries:ident ($($key:tt)+) (:) $copy:tt) => {
		$crate::internal!();
	};

	(@entries $entries:ident ($($key:tt)+) () $copy:tt) => {
		$crate::internal!();
	};

	(@entries $entries:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
		$crate::artifact_unexpected!($colon);
	};

	(@entries $entries:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
		$crate::artifact_unexpected!($comma);
	};

	(@entries $entries:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
		$crate::internal!(@entries $entries ($key) (: $($rest)*) (: $($rest)*));
	};

	(@entries $entries:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
		$crate::expect_expr_comma!($($unexpected)+);
	};

	(@entries $entries:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
		$crate::internal!(@entries $entries ($($key)* $tt) ($($rest)*) ($($rest)*));
	};

	({ $($tt:tt)+ }) => {
		$crate::artifact::Artifact::Directory {
			entries: {
				let mut entries = std::collections::BTreeMap::new();
				$crate::internal!(@entries entries () ($($tt)+) ($($tt)+));
				entries
			}
		}
	};

	($other:expr) => {
		$crate::artifact::Artifact::from($other)
	};
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
		$crate::artifact::Artifact::Symlink { target: $target }
	}};
}

#[macro_export]
#[doc(hidden)]
macro_rules! unexpected {
	() => {};
}

#[macro_export]
#[doc(hidden)]
macro_rules! expect_expr_comma {
	($e:expr , $($tt:tt)*) => {};
}
