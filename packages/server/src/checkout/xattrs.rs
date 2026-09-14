use {
	std::{fs::OpenOptions, path::Path},
	tangram_client::prelude::*,
	xattr::FileExt as _,
};

const PROBE_XATTR_NAME: &str = "user.tangram.probe";

pub fn internal_capabilities(server: &crate::Server) -> tg::Result<tg::file::xattrs::Options> {
	let mut cached = server.checkout_xattr_value_size.lock().unwrap();
	let max_value_size = if let Some(max_value_size) = *cached {
		max_value_size
	} else {
		let path = server.checkout_path();
		let max_value_size = probe(&path)?.max_value_size;
		cached.replace(max_value_size);
		max_value_size
	};

	Ok(tg::file::xattrs::Options { max_value_size })
}

pub fn probe(directory: &Path) -> tg::Result<tg::file::xattrs::Options> {
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

	Ok(tg::file::xattrs::Options { max_value_size })
}

fn is_capacity_error(error: &std::io::Error) -> bool {
	matches!(
		error.raw_os_error(),
		Some(libc::E2BIG | libc::EDQUOT | libc::ENOSPC | libc::ERANGE)
	)
}
