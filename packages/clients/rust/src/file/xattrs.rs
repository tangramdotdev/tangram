use {crate::prelude::*, std::path::Path};

mod dependencies;
#[cfg(test)]
mod tests;

pub use self::dependencies::{
	DependenciesXattr, dependencies_xattrs, deserialize_dependencies_xattr,
	is_dependencies_xattr_name, try_read_dependencies_xattrs,
};

#[derive(Clone, Copy, Debug)]
pub struct Arg<'a> {
	/// `None` removes the attributes; `Some(&[])` writes an empty list.
	pub dependencies: Option<&'a [tg::Reference]>,

	/// Only the listed attributes are modified; `None` removes an attribute.
	pub required: &'a [(&'a str, Option<&'a [u8]>)],

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

pub fn read(path: impl AsRef<Path>) -> tg::Result<Output> {
	// List the attributes.
	let path = path.as_ref();
	let names = xattr::list_deref(path)
		.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to list the checkout xattrs"),
		)?
		.collect::<Vec<_>>();
	let has_token = names.iter().any(|name| name == tg::file::TOKEN_XATTR_NAME);

	// Read the dependencies.
	let dependencies = try_read_dependencies_xattrs(names, |name| xattr::get_deref(path, name))?;

	// Read the token.
	let token = if has_token {
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

	// Remove the optional xattrs.
	remove_dependencies_xattrs(path)?;
	remove_xattr(path, tg::file::TOKEN_XATTR_NAME)?;

	// Write the required xattrs first to reserve space for them.
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
	remove_xattr(path, tg::file::TOKEN_XATTR_NAME)?;
	if !write_dependencies_xattrs(path, &dependencies, options.max_value_size)? {
		return Err(tg::error!(
			"the dependencies do not fit in the available xattr space"
		));
	}

	Ok(())
}

pub fn deserialize_token_xattr(value: &[u8]) -> tg::Result<tg::authorization::Token> {
	let value = std::str::from_utf8(value)
		.map_err(|error| tg::error!(!error, "the file token xattr is not valid utf-8"))?;
	let token = value
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the file token xattr"))?;
	Ok(token)
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
