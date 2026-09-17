use std::{collections::BTreeMap, io, path::Path};

#[cfg(test)]
mod tests;

/// Read an attribute stored as a single value or numbered shards.
pub fn get(path: impl AsRef<Path>, name: &str) -> io::Result<Option<Vec<u8>>> {
	let path = path.as_ref();
	let value = xattr::get(path, name)?;
	let prefix = format!("{name}.");
	let mut shards = BTreeMap::new();
	for name in xattr::list(path)? {
		let Some(name) = name.to_str() else {
			continue;
		};
		let Some(suffix) = name.strip_prefix(&prefix) else {
			continue;
		};
		let index = suffix
			.parse::<usize>()
			.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
		if suffix != index.to_string() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"invalid xattr shard name",
			));
		}
		shards.insert(index, name.to_owned());
	}
	if shards.is_empty() {
		return Ok(value);
	}
	if value.is_some() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"found both unsharded and sharded xattrs",
		));
	}
	let mut value = Vec::new();
	for (expected, (index, name)) in shards.into_iter().enumerate() {
		if index != expected {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"found a gap in the xattr shards",
			));
		}
		let shard = xattr::get(path, name)?.ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidData, "an xattr shard disappeared")
		})?;
		value.extend(shard);
	}

	Ok(Some(value))
}

/// Write an attribute, splitting the value when the filesystem limits its size.
pub fn set(path: impl AsRef<Path>, name: &str, value: &[u8]) -> io::Result<()> {
	let path = path.as_ref();
	remove(path, name)?;
	match xattr::set(path, name, value) {
		Ok(()) => return Ok(()),
		Err(error) if is_capacity_error(&error) && value.len() > 1 => {},
		Err(error) => return Err(error),
	}

	let mut size = (value.len() / 2).min(8192);
	loop {
		let result = value
			.chunks(size)
			.enumerate()
			.try_for_each(|(index, value)| xattr::set(path, format!("{name}.{index}"), value));
		let Err(error) = result else {
			return Ok(());
		};
		remove(path, name)?;
		if !is_capacity_error(&error) || size == 1 {
			return Err(error);
		}
		size /= 2;
	}
}

fn remove(path: &Path, name: &str) -> io::Result<()> {
	let prefix = format!("{name}.");
	for attribute in xattr::list(path)? {
		if attribute == name
			|| attribute
				.to_str()
				.is_some_and(|name| name.starts_with(&prefix))
		{
			xattr::remove(path, attribute)?;
		}
	}
	Ok(())
}

fn is_capacity_error(error: &io::Error) -> bool {
	matches!(
		error.raw_os_error(),
		Some(libc::E2BIG | libc::ENOSPC | libc::ERANGE)
	)
}
