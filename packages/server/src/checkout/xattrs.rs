use {
	std::{fs::OpenOptions, path::Path},
	tangram_client::prelude::*,
	xattr::FileExt as _,
};

const PROBE_XATTR_NAME: &str = "user.tangram.probe";

#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
	pub max_value_size: usize,
}

#[derive(Clone, Copy)]
pub struct Xattrs<'a> {
	pub dependencies: &'a [tg::Reference],
	pub required: &'a [(&'a str, Option<&'a [u8]>)],
	pub token: Option<&'a tg::authorization::Token>,
}

pub fn internal_capabilities(server: &crate::Server) -> tg::Result<Capabilities> {
	let mut cached = server.checkout_xattr_value_size.lock().unwrap();
	let max_value_size = if let Some(max_value_size) = *cached {
		max_value_size
	} else {
		let path = server.checkout_path();
		let max_value_size = probe(&path)?.max_value_size;
		cached.replace(max_value_size);
		max_value_size
	};

	Ok(Capabilities { max_value_size })
}

pub fn probe(directory: &Path) -> tg::Result<Capabilities> {
	// Create the probe file.
	let name = format!(".tangram-xattr-probe-{}", uuid::Uuid::now_v7());
	let path = directory.join(name);
	let file = OpenOptions::new()
		.create_new(true)
		.write(true)
		.open(&path)
		.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the xattr probe file"),
		)?;
	std::fs::remove_file(&path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to unlink the xattr probe file"),
	)?;

	// Find a supported value size.
	let value = vec![0; tg::file::DEPENDENCIES_XATTR_VALUE_SIZE];
	let mut max_value_size = value.len();
	loop {
		match file.set_xattr(PROBE_XATTR_NAME, &value[..max_value_size]) {
			Ok(()) => break,
			Err(error) if is_capacity_error(&error) && max_value_size > 1 => {
				max_value_size /= 2;
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %path.display(),
					"failed to probe the xattr value size"
				));
			},
		}
	}

	Ok(Capabilities { max_value_size })
}

pub fn write_file_xattrs(
	path: &Path,
	xattrs: Xattrs<'_>,
	capabilities: Capabilities,
) -> tg::Result<()> {
	let Xattrs {
		dependencies: references,
		required,
		token,
	} = xattrs;

	// Remove optional xattrs that may have been copied with the file.
	remove_dependencies_xattrs(path)?;
	remove_xattr(path, tg::file::TOKEN_XATTR_NAME)?;

	// Remove replaced attributes before reserving space for the required xattrs.
	for (name, _) in required {
		remove_xattr(path, name)?;
	}
	for (name, value) in required {
		if let Some(value) = value {
			xattr::set(path, name, value).map_err(
				|error| tg::error!(!error, %name, "failed to write a required file xattr"),
			)?;
		}
	}

	// Write the file token.
	let token_written = if let Some(token) = token {
		let value = token.to_string();
		match xattr::set(path, tg::file::TOKEN_XATTR_NAME, value.as_bytes()) {
			Ok(()) => true,
			Err(error) if is_capacity_error(&error) => false,
			Err(error) => {
				return Err(tg::error!(!error, "failed to write the file token xattr"));
			},
		}
	} else {
		false
	};

	if references.is_empty() {
		return Ok(());
	}

	// Try the representations that retain the file token.
	if token.is_none() || token_written {
		if write_dependencies_xattrs(path, references, capabilities.max_value_size)? {
			return Ok(());
		}
		let references = references
			.iter()
			.cloned()
			.map(tg::Reference::without_tokens)
			.collect::<Vec<_>>();
		if write_dependencies_xattrs(path, &references, capabilities.max_value_size)? {
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
	if write_dependencies_xattrs(path, &references, capabilities.max_value_size)? {
		return Ok(());
	}

	Err(tg::error!(
		"the dependencies do not fit in the available xattr space"
	))
}

fn write_dependencies_xattrs(
	path: &Path,
	references: &[tg::Reference],
	mut max_value_size: usize,
) -> tg::Result<bool> {
	loop {
		let xattrs = tg::file::dependencies_xattrs(references, max_value_size)?;
		let mut error = None;
		for xattr in xattrs {
			if let Err(inner) = xattr::set(path, &xattr.name, &xattr.value) {
				error.replace(inner);
				break;
			}
		}
		let Some(error) = error else {
			return Ok(true);
		};
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
	let names = xattr::list(path)
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
	match xattr::remove(path, name) {
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
