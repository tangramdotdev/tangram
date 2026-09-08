//! Read the references retained by a checked-out file.

use {
	crate::prelude::*,
	std::{collections::BTreeMap, fs::File, path::Path},
	xattr::FileExt as _,
};

/// The authorization metadata retained by a checked-out file.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct Output {
	/// The original dependency references, including their locations, options, and tokens.
	/// `None` means no dependency metadata was present; `Some(vec![])` is an explicit empty list.
	pub dependencies: Option<Vec<tg::Reference>>,

	/// The file's own token, kept separate from the dependency tokens without inheritance or replacement.
	pub token: Option<tg::authorization::Token>,
}

/// Read checkout metadata from a file on a regular filesystem or a Tangram VFS mount.
///
/// This follows symlinks and reads through one open file descriptor. It does not check in the
/// path, load an object, initialize a client, or make a client authorization request. Filesystem
/// operations on a VFS mount may communicate with its provider.
///
/// Missing attributes are represented by `None`. Invalid or unreadable metadata is an error.
/// Tokens are parsed without checking their signatures or expiration and are returned intact.
pub fn read(path: impl AsRef<Path>) -> tg::Result<Output> {
	let path = path.as_ref();
	let file = File::open(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to open the checkout"),
	)?;
	read_with_file(&file).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to read the checkout metadata"),
	)
}

fn read_with_file(file: &File) -> tg::Result<Output> {
	// Read the attribute names from the same file as their values.
	let metadata = file
		.metadata()
		.map_err(|error| tg::error!(!error, "failed to get the file metadata"))?;
	if !metadata.is_file() {
		return Err(tg::error!("expected a file"));
	}
	let names = file
		.list_xattr()
		.map_err(|error| tg::error!(!error, "failed to list the checkout attributes"))?;
	let mut base = false;
	let mut shards = BTreeMap::new();
	let mut token = false;
	for name in names {
		let Some(name) = name.to_str() else { continue };
		if name == tg::file::TOKEN_XATTR_NAME {
			token = true;
		} else if name == tg::file::DEPENDENCIES_XATTR_NAME {
			base = true;
		} else if let Some(suffix) = name
			.strip_prefix(tg::file::DEPENDENCIES_XATTR_NAME)
			.and_then(|suffix| suffix.strip_prefix('.'))
		{
			let index = suffix
				.parse::<usize>()
				.map_err(|error| tg::error!(!error, "invalid dependency shard index"))?;
			if suffix != index.to_string() {
				return Err(tg::error!("invalid dependency shard index"));
			}
			shards.insert(index, name.to_owned());
		}
	}

	// Reconstruct the dependency references without dropping options or tokens.
	if base {
		if !shards.is_empty() {
			return Err(tg::error!("mixed dependency attributes"));
		}
		shards.insert(0, tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
	}
	let dependencies = if shards.is_empty() {
		None
	} else {
		let mut bytes = Vec::new();
		for (expected, (index, name)) in shards.into_iter().enumerate() {
			if index != expected {
				return Err(tg::error!("missing a dependency shard"));
			}
			bytes.extend(read_attribute(file, &name)?);
		}
		Some(tg::file::deserialize_dependencies_xattr(&bytes)?)
	};

	// Keep the file token separate from the tokens on its dependencies.
	let token = if token {
		let bytes = read_attribute(file, tg::file::TOKEN_XATTR_NAME)?;
		let value = std::str::from_utf8(&bytes)
			.map_err(|error| tg::error!(!error, "invalid file token encoding"))?;
		Some(
			value
				.parse::<tg::authorization::Token>()
				.map_err(|error| tg::error!(!error, "failed to parse the file token"))?,
		)
	} else {
		None
	};
	let output = Output {
		dependencies,
		token,
	};

	Ok(output)
}

fn read_attribute(file: &File, name: &str) -> tg::Result<Vec<u8>> {
	file.get_xattr(name)
		.map_err(|error| tg::error!(!error, %name, "failed to read a checkout attribute"))?
		.ok_or_else(|| tg::error!(%name, "a checkout attribute disappeared"))
}
