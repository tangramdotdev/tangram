use {crate::prelude::*, std::path::Path};

mod dependencies;
mod shards;
#[cfg(test)]
mod tests;

pub use self::dependencies::{Entry, encode_dependencies, is_dependencies_name};

pub const CHECKSUM_NAME: &str = "user.tangram.checksum";
pub const DEPENDENCIES_NAME: &str = "user.tangram.dependencies";
pub const ERROR_NAME: &str = "user.tangram.error";
pub const LOCK_NAME: &str = "user.tangram.lock";
/// The maximum extended attribute value size used by virtual filesystems.
pub const MAX_VALUE_SIZE: usize = 64 * 1024;
pub const MODULE_NAME: &str = "user.tangram.module";
pub const OUTPUT_NAME: &str = "user.tangram.output";
pub const TOKEN_NAME: &str = "user.tangram.token";

#[derive(Clone, Copy, Debug)]
pub struct Arg<'a> {
	/// `None` removes the attributes; `Some(&[])` writes an empty list.
	pub dependencies: Option<&'a [tg::Reference]>,

	/// Only the listed attributes are modified; `None` removes an attribute.
	pub required: &'a [Required<'a>],

	pub token: Option<&'a tg::authorization::Token>,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
	/// The initial maximum dependency shard size, in bytes.
	pub max_value_size: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Output {
	pub dependencies: Option<Vec<tg::Reference>>,
	pub token: Option<tg::authorization::Token>,
}

#[derive(Clone, Copy, Debug)]
pub enum Required<'a> {
	Lock(Option<&'a [u8]>),
	Module(Option<&'a [u8]>),
}

/// Read checkout metadata, following symlinks and reporting filesystem errors.
pub fn read(path: impl AsRef<Path>) -> tg::Result<Output> {
	// List the attributes.
	let path = path.as_ref();
	let names = xattr::list_deref(path)
		.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to list the checkout xattrs"),
		)?
		.collect::<Vec<_>>();
	let has_token = names
		.iter()
		.any(|name| name == tg::file::xattrs::TOKEN_NAME);

	// Read the dependencies.
	let dependencies =
		dependencies::try_read_dependencies_xattrs(names, |name| xattr::get_deref(path, name))?;

	// Read the token.
	let token = if has_token {
		let value = xattr::get_deref(path, tg::file::xattrs::TOKEN_NAME)
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

/// Write checkout metadata through symlinks, reserving space for required attributes before tokens.
/// The write may omit tokens to fit the xattr limits and may leave partial metadata on error.
pub fn write(path: impl AsRef<Path>, arg: Arg<'_>, options: Options) -> tg::Result<()> {
	// Validate the arg.
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

	// Remove the optional xattrs.
	remove_dependencies_xattrs(path)?;
	remove_xattr(path, tg::file::xattrs::TOKEN_NAME)?;

	// Write the required xattrs first to reserve space for them.
	for attribute in required {
		let (name, _) = attribute.parts();
		remove_xattr(path, name)?;
	}
	for attribute in required {
		let (name, value) = attribute.parts();
		if let Some(value) = value {
			xattr::set_deref(path, name, value).map_err(
				|error| tg::error!(!error, %name, "failed to write a required file xattr"),
			)?;
		}
	}

	// Write the file token.
	let token_written = if let Some(token) = token {
		let value = token.to_string();
		match xattr::set_deref(path, tg::file::xattrs::TOKEN_NAME, value.as_bytes()) {
			Ok(()) => true,
			Err(error) if is_capacity_error(&error) => false,
			Err(error) => {
				return Err(tg::error!(!error, "failed to write the file token xattr"));
			},
		}
	} else {
		false
	};

	// Write the dependencies.
	let Some(dependencies) = dependencies else {
		return Ok(());
	};
	if (token.is_none() || token_written)
		&& write_dependencies_xattrs(path, dependencies, options.max_value_size)?
	{
		return Ok(());
	}

	// Omit the dependency tokens.
	let dependencies = dependencies
		.iter()
		.cloned()
		.map(tg::Reference::without_tokens)
		.collect::<Vec<_>>();
	if (token.is_none() || token_written)
		&& write_dependencies_xattrs(path, &dependencies, options.max_value_size)?
	{
		return Ok(());
	}

	// Omit the file token.
	remove_xattr(path, tg::file::xattrs::TOKEN_NAME)?;
	if !write_dependencies_xattrs(path, &dependencies, options.max_value_size)? {
		return Err(tg::error!(
			"the dependencies do not fit in the available xattr space"
		));
	}

	Ok(())
}

pub fn read_checksum(path: impl AsRef<Path>) -> tg::Result<Option<Vec<u8>>> {
	let value = xattr::get(path, CHECKSUM_NAME)
		.map_err(|error| tg::error!(!error, "failed to read the checksum xattr"))?;
	Ok(value)
}

pub fn write_checksum(path: impl AsRef<Path>, value: &[u8]) -> tg::Result<()> {
	xattr::set(path, CHECKSUM_NAME, value)
		.map_err(|error| tg::error!(!error, "failed to write the checksum xattr"))?;
	Ok(())
}

pub fn read_error(path: impl AsRef<Path>) -> tg::Result<Option<Vec<u8>>> {
	let value = shards::read_sharded(path, ERROR_NAME)
		.map_err(|error| tg::error!(!error, "failed to read the error xattr"))?;
	Ok(value)
}

pub fn write_error(path: impl AsRef<Path>, value: &[u8]) -> tg::Result<()> {
	shards::write_sharded(path, ERROR_NAME, value)
		.map_err(|error| tg::error!(!error, "failed to write the error xattr"))?;
	Ok(())
}

pub fn read_lock(path: impl AsRef<Path>) -> tg::Result<Option<Vec<u8>>> {
	let value = xattr::get(path, LOCK_NAME)
		.map_err(|error| tg::error!(!error, "failed to read the lock xattr"))?;
	Ok(value)
}

pub fn write_lock(path: impl AsRef<Path>, value: &[u8]) -> tg::Result<()> {
	xattr::set(path, LOCK_NAME, value)
		.map_err(|error| tg::error!(!error, "failed to write the lock xattr"))?;
	Ok(())
}

pub fn read_output(path: impl AsRef<Path>) -> tg::Result<Option<Vec<u8>>> {
	let value = shards::read_sharded(path, OUTPUT_NAME)
		.map_err(|error| tg::error!(!error, "failed to read the output xattr"))?;
	Ok(value)
}

pub fn write_output(path: impl AsRef<Path>, value: &[u8]) -> tg::Result<()> {
	shards::write_sharded(path, OUTPUT_NAME, value)
		.map_err(|error| tg::error!(!error, "failed to write the output xattr"))?;
	Ok(())
}

/// Read optional checkin metadata, treating an inaccessible attribute list as absent.
pub fn read_dependencies(path: impl AsRef<Path>) -> tg::Result<Option<Vec<tg::Reference>>> {
	let path = path.as_ref();
	let Ok(names) = xattr::list(path) else {
		return Ok(None);
	};
	dependencies::try_read_dependencies_xattrs(names, |name| xattr::get(path, name))
}

/// Replace dependencies through symlinks, omitting their tokens only if required to fit the filesystem.
pub fn write_dependencies(
	path: impl AsRef<Path>,
	references: &[tg::Reference],
	options: Options,
) -> tg::Result<()> {
	let path = path.as_ref();
	if options.max_value_size == 0 {
		return Err(tg::error!(
			"the maximum xattr value size must be greater than zero"
		));
	}
	remove_dependencies_xattrs(path)?;
	if write_dependencies_xattrs(path, references, options.max_value_size)? {
		return Ok(());
	}
	let references = references
		.iter()
		.cloned()
		.map(tg::Reference::without_tokens)
		.collect::<Vec<_>>();
	if !write_dependencies_xattrs(path, &references, options.max_value_size)? {
		return Err(tg::error!(
			"the dependencies do not fit in the available xattr space"
		));
	}
	Ok(())
}

pub fn read_module(path: impl AsRef<Path>) -> tg::Result<Option<tg::module::Kind>> {
	let Some(value) = xattr::get(path, MODULE_NAME)
		.map_err(|error| tg::error!(!error, "failed to read the module xattr"))?
	else {
		return Ok(None);
	};
	let value = std::str::from_utf8(&value)
		.map_err(|error| tg::error!(!error, "the module xattr is not valid utf-8"))?;
	let kind = value
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the module kind"))?;
	Ok(Some(kind))
}

pub fn write_module(path: impl AsRef<Path>, kind: tg::module::Kind) -> tg::Result<()> {
	xattr::set(path, MODULE_NAME, kind.to_string().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to write the module xattr"))?;
	Ok(())
}

/// Read an optional checkin token, treating an inaccessible attribute as absent.
pub fn read_token(path: impl AsRef<Path>) -> tg::Result<Option<tg::authorization::Token>> {
	let Ok(Some(value)) = xattr::get(path, TOKEN_NAME) else {
		return Ok(None);
	};
	let token = deserialize_token_xattr(&value)?;
	Ok(Some(token))
}

pub fn write_token(path: impl AsRef<Path>, token: &tg::authorization::Token) -> tg::Result<()> {
	xattr::set(path, TOKEN_NAME, token.to_string().as_bytes())
		.map_err(|error| tg::error!(!error, "failed to write the file token xattr"))?;
	Ok(())
}

pub fn remove_lock(path: impl AsRef<Path>) -> tg::Result<()> {
	match xattr::remove(path, LOCK_NAME) {
		Ok(()) => {},
		Err(error) if is_missing_xattr_error(&error) => {},
		Err(error) => return Err(tg::error!(!error, "failed to remove the lock xattr")),
	}
	Ok(())
}

impl<'a> Required<'a> {
	fn parts(&self) -> (&'static str, Option<&'a [u8]>) {
		match self {
			Self::Lock(value) => (LOCK_NAME, *value),
			Self::Module(value) => (MODULE_NAME, *value),
		}
	}
}

impl Default for Options {
	fn default() -> Self {
		Self {
			max_value_size: tg::file::xattrs::MAX_VALUE_SIZE,
		}
	}
}

fn deserialize_token_xattr(value: &[u8]) -> tg::Result<tg::authorization::Token> {
	let value = std::str::from_utf8(value)
		.map_err(|error| tg::error!(!error, "the file token xattr is not valid utf-8"))?;
	let token = value
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the file token xattr"))?;
	Ok(token)
}

fn write_dependencies_xattrs(
	path: &Path,
	references: &[tg::Reference],
	mut max_value_size: usize,
) -> tg::Result<bool> {
	loop {
		// Write the dependency shards.
		let xattrs = tg::file::xattrs::encode_dependencies(references, max_value_size)?;
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

		// Remove the partial xattrs before retrying.
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
		if !tg::file::xattrs::is_dependencies_name(name) {
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
