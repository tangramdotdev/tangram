use {
	crate::prelude::*,
	bytes::Bytes,
	std::{collections::BTreeMap, ffi::OsString},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
	pub name: String,
	pub value: Bytes,
}

pub fn encode_dependencies(
	references: &[tg::Reference],
	max_value_size: usize,
) -> tg::Result<Vec<Entry>> {
	// Validate the shard size.
	if max_value_size == 0 {
		return Err(tg::error!(
			"the maximum xattr value size must be greater than zero"
		));
	}

	// Serialize the references into shards.
	let value = serde_json::to_vec(references)
		.map_err(|error| tg::error!(!error, "failed to serialize the dependencies"))?;
	let count = value.len().div_ceil(max_value_size);
	let xattrs = value
		.chunks(max_value_size)
		.enumerate()
		.map(|(index, value)| {
			let name = if count == 1 {
				tg::file::xattrs::DEPENDENCIES_NAME.to_owned()
			} else {
				format!("{}.{index}", tg::file::xattrs::DEPENDENCIES_NAME)
			};
			let value = Bytes::copy_from_slice(value);
			Entry { name, value }
		})
		.collect();

	Ok(xattrs)
}

pub(super) fn deserialize_dependencies_xattr(value: &[u8]) -> tg::Result<Vec<tg::Reference>> {
	serde_json::from_slice(value)
		.map_err(|error| tg::error!(!error, "failed to deserialize the dependencies"))
}

#[must_use]
pub fn is_dependencies_name(name: &str) -> bool {
	if name == tg::file::xattrs::DEPENDENCIES_NAME {
		return true;
	}
	let Some(suffix) = name
		.strip_prefix(tg::file::xattrs::DEPENDENCIES_NAME)
		.and_then(|suffix| suffix.strip_prefix('.'))
	else {
		return false;
	};
	!suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
}

/// Read the dependency references from the listed attributes.
pub(super) fn try_read_dependencies_xattrs(
	names: impl IntoIterator<Item = OsString>,
	mut read: impl FnMut(&str) -> std::io::Result<Option<Vec<u8>>>,
) -> tg::Result<Option<Vec<tg::Reference>>> {
	// Order the shards by their numeric indices.
	let mut base = false;
	let mut shards = BTreeMap::new();
	for name in names {
		let Some(name) = name.to_str() else {
			continue;
		};
		if name == tg::file::xattrs::DEPENDENCIES_NAME {
			base = true;
			continue;
		}
		let Some(suffix) = name
			.strip_prefix(tg::file::xattrs::DEPENDENCIES_NAME)
			.and_then(|suffix| suffix.strip_prefix('.'))
		else {
			continue;
		};
		let index = suffix
			.parse::<usize>()
			.map_err(|error| tg::error!(!error, %name, "invalid dependencies xattr name"))?;
		if suffix != index.to_string() {
			return Err(tg::error!(%name, "invalid dependencies xattr name"));
		}
		shards.insert(index, name.to_owned());
	}
	if base && !shards.is_empty() {
		return Err(tg::error!(
			"found both unsharded and sharded dependencies xattrs"
		));
	}
	if base {
		shards.insert(0, tg::file::xattrs::DEPENDENCIES_NAME.to_owned());
	}
	if shards.is_empty() {
		return Ok(None);
	}

	// Concatenate the shards and deserialize the references.
	let mut value = Vec::new();
	for (expected, (index, name)) in shards.into_iter().enumerate() {
		if index != expected {
			return Err(tg::error!("found a gap in the dependencies xattr shards"));
		}
		let shard = read(&name)
			.map_err(|error| tg::error!(!error, %name, "failed to read a dependencies xattr"))?
			.ok_or_else(|| tg::error!(%name, "a dependencies xattr disappeared"))?;
		value.extend(shard);
	}
	let references = deserialize_dependencies_xattr(&value)?;

	Ok(Some(references))
}
