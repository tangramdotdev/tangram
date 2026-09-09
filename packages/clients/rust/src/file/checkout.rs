use {
	crate::prelude::*,
	std::{fs::File, path::Path},
	xattr::FileExt as _,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Output {
	/// The retained dependency references, or `None` if the attribute is absent.
	pub dependencies: Option<Vec<tg::Reference>>,

	/// The file's authorization token.
	pub token: Option<tg::authorization::Token>,
}

/// Read a file's checkout metadata, following symlinks.
///
/// Missing attributes are `None`; malformed or unreadable metadata is an error.
/// Tokens are returned without signature or expiration validation.
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
	// Read the attribute names.
	let metadata = file
		.metadata()
		.map_err(|error| tg::error!(!error, "failed to get the file metadata"))?;
	if !metadata.is_file() {
		return Err(tg::error!("expected a file"));
	}
	let names = file
		.list_xattr()
		.map_err(|error| tg::error!(!error, "failed to list the checkout attributes"))?
		.collect::<Vec<_>>();
	let token = names.iter().any(|name| name == tg::file::TOKEN_XATTR_NAME);

	// Read the dependencies.
	let dependencies = tg::file::try_read_dependencies_xattrs(names, |name| file.get_xattr(name))?;

	// Read the file token.
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
