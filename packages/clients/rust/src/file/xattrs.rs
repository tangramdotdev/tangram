//! Read and write Tangram's public dependency and authorization xattr schema.
//!
//! `user.tangram.dependencies` contains a JSON array of reference strings.
//! Larger values use `user.tangram.dependencies.0`, `.1`, and so on, whose bytes are concatenated in numeric order.
//! The shard indices must be contiguous and canonical, and the unsharded and sharded forms cannot coexist.
//! `user.tangram.token` contains a UTF-8 authorization token; parsing does not verify its signature or expiration.
//!
//! The writer accepts required attributes as encoded bytes so they share the filesystem's space budget with dependencies and tokens.

use {crate::prelude::*, std::path::Path};

mod dependencies;
#[cfg(test)]
mod tests;

pub use self::dependencies::{
	DependenciesXattr, dependencies_xattrs, deserialize_dependencies_xattr,
	is_dependencies_xattr_name, try_read_dependencies_xattrs,
};

/// The metadata to write to a checkout.
#[derive(Clone, Copy, Debug)]
pub struct Arg<'a> {
	/// The dependency references and their tokens; `None` removes the attributes, and `Some(&[])` writes an empty list.
	pub dependencies: Option<&'a [tg::Reference]>,
	/// The required attributes, such as the module kind and serialized lock.
	/// A `None` value removes the attribute, and the unlisted attributes are preserved.
	/// The dependency and token attributes must use their dedicated fields.
	pub required: &'a [(&'a str, Option<&'a [u8]>)],
	/// The file's authorization token.
	pub token: Option<&'a tg::authorization::Token>,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
	/// The initial maximum dependency shard size, in bytes.
	pub max_value_size: usize,
}

/// The dependency and authorization metadata recovered from a checkout.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Output {
	/// The retained dependency references, or `None` if the attributes are absent.
	pub dependencies: Option<Vec<tg::Reference>>,
	/// The file's authorization token, without signature or expiration validation.
	pub token: Option<tg::authorization::Token>,
}

/// Read the dependency and authorization xattrs, following symlinks.
/// `None` represents an absent attribute; malformed or unreadable metadata returns an error.
pub fn read(path: impl AsRef<Path>) -> tg::Result<Output> {
	// List the attributes.
	let path = path.as_ref();
	let names = xattr::list_deref(path)
		.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to list the checkout xattrs"),
		)?
		.collect::<Vec<_>>();
	let token = names.iter().any(|name| name == tg::file::TOKEN_XATTR_NAME);

	// Read the dependencies and the file token.
	let dependencies = try_read_dependencies_xattrs(names, |name| xattr::get_deref(path, name))?;
	let token = if token {
		let value = xattr::get_deref(path, tg::file::TOKEN_XATTR_NAME)
			.map_err(|error| tg::error!(!error, "failed to read the file token xattr"))?
			.ok_or_else(|| tg::error!("the file token xattr disappeared"))?;
		Some(deserialize_token_xattr(&value)?)
	} else {
		None
	};

	// Create the output.
	let output = Output {
		dependencies,
		token,
	};

	Ok(output)
}

/// Parse a token xattr without verifying its signature or expiration.
pub fn deserialize_token_xattr(value: &[u8]) -> tg::Result<tg::authorization::Token> {
	let value = std::str::from_utf8(value)
		.map_err(|error| tg::error!(!error, "the file token xattr is not valid utf-8"))?;
	let token = value
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the file token xattr"))?;
	Ok(token)
}

/// Write the checkout xattrs, following symlinks and replacing the previous dependency shards.
/// The required attributes take priority, followed by the dependency references and then the tokens.
/// The tokens may be omitted when the filesystem lacks space, but the dependency references are retained.
/// An error may leave partially written metadata.
pub fn write(path: impl AsRef<Path>, arg: Arg<'_>, options: Options) -> tg::Result<()> {
	// Validate the arg and options before modifying the file.
	let path = path.as_ref();
	if options.max_value_size == 0 {
		return Err(tg::error!(
			"the maximum xattr value size must be greater than zero"
		));
	}
	let Arg {
		dependencies,
		required,
		token,
	} = arg;
	for (name, _) in required {
		if *name == tg::file::TOKEN_XATTR_NAME
			|| *name == tg::file::DEPENDENCIES_XATTR_NAME
			|| name.starts_with(&format!("{}.", tg::file::DEPENDENCIES_XATTR_NAME))
		{
			return Err(
				tg::error!(%name, "a required xattr conflicts with the dependency or token metadata"),
			);
		}
	}

	// Remove the optional xattrs that may have been copied with the file.
	remove_dependencies_xattrs(path)?;
	remove_xattr(path, tg::file::TOKEN_XATTR_NAME)?;

	// Reserve space for the required attributes before writing the optional metadata.
	for (name, _) in required {
		remove_xattr(path, name)?;
	}
	for (name, value) in required {
		if let Some(value) = value {
			xattr::set_deref(path, name, value).map_err(
				|error| tg::error!(!error, %name, "failed to write a required file xattr"),
			)?;
		}
	}

	// Write the file token.
	let token_written = if let Some(token) = token {
		let value = token.to_string();
		match xattr::set_deref(path, tg::file::TOKEN_XATTR_NAME, value.as_bytes()) {
			Ok(()) => true,
			Err(error) if is_capacity_error(&error) => false,
			Err(error) => {
				return Err(tg::error!(!error, "failed to write the file token xattr"));
			},
		}
	} else {
		false
	};

	let Some(references) = dependencies else {
		return Ok(());
	};

	// Try the representations that retain the file token.
	if token.is_none() || token_written {
		if write_dependencies_xattrs(path, references, options.max_value_size)? {
			return Ok(());
		}
		let references = references
			.iter()
			.cloned()
			.map(tg::Reference::without_tokens)
			.collect::<Vec<_>>();
		if write_dependencies_xattrs(path, &references, options.max_value_size)? {
			return Ok(());
		}
	}

	// Make one final attempt without any authorization tokens.
	remove_xattr(path, tg::file::TOKEN_XATTR_NAME)?;
	let references = references
		.iter()
		.cloned()
		.map(tg::Reference::without_tokens)
		.collect::<Vec<_>>();
	if write_dependencies_xattrs(path, &references, options.max_value_size)? {
		return Ok(());
	}

	Err(tg::error!(
		"the dependencies do not fit in the available xattr space"
	))
}

impl Default for Options {
	fn default() -> Self {
		Self {
			max_value_size: tg::file::DEPENDENCIES_XATTR_VALUE_SIZE,
		}
	}
}

fn write_dependencies_xattrs(
	path: &Path,
	references: &[tg::Reference],
	mut max_value_size: usize,
) -> tg::Result<bool> {
	loop {
		// Write the dependency shards.
		let xattrs = tg::file::dependencies_xattrs(references, max_value_size)?;
		let mut error = None;
		for xattr in xattrs {
			if let Err(inner) = xattr::set_deref(path, &xattr.name, &xattr.value) {
				error.replace(inner);
				break;
			}
		}
		let Some(error) = error else {
			return Ok(true);
		};

		// Remove the partial metadata before retrying with smaller shards or fewer tokens.
		remove_dependencies_xattrs(path)?;
		if is_value_size_error(&error) && max_value_size > 1 {
			max_value_size /= 2;
			continue;
		}
		if is_capacity_error(&error) {
			return Ok(false);
		}

		return Err(tg::error!(!error, "failed to write a dependencies xattr"));
	}
}

fn remove_dependencies_xattrs(path: &Path) -> tg::Result<()> {
	let names = xattr::list_deref(path)
		.map_err(|error| tg::error!(!error, "failed to list the file's xattrs"))?;
	for name in names {
		let Some(name) = name.to_str() else {
			continue;
		};
		if !tg::file::is_dependencies_xattr_name(name) {
			continue;
		}
		remove_xattr(path, name)?;
	}

	Ok(())
}

fn remove_xattr(path: &Path, name: &str) -> tg::Result<()> {
	match xattr::remove_deref(path, name) {
		Ok(()) => {},
		Err(error) if is_missing_xattr_error(&error) => {},
		Err(error) => return Err(tg::error!(!error, %name, "failed to remove a file xattr")),
	}
	Ok(())
}

fn is_capacity_error(error: &std::io::Error) -> bool {
	matches!(
		error.raw_os_error(),
		Some(libc::E2BIG | libc::EDQUOT | libc::ENOSPC | libc::ERANGE)
	)
}

fn is_value_size_error(error: &std::io::Error) -> bool {
	matches!(error.raw_os_error(), Some(libc::E2BIG | libc::ERANGE))
}

fn is_missing_xattr_error(error: &std::io::Error) -> bool {
	if error.kind() == std::io::ErrorKind::NotFound {
		return true;
	}
	#[cfg(target_os = "linux")]
	if error.raw_os_error() == Some(libc::ENODATA) {
		return true;
	}
	#[cfg(target_os = "macos")]
	if error.raw_os_error() == Some(libc::ENOATTR) {
		return true;
	}
	false
}
