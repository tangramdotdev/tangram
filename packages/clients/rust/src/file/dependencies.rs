use {
	crate::prelude::*,
	bytes::Bytes,
	std::{collections::BTreeMap, ffi::OsString},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependenciesXattr {
	pub name: String,
	pub value: Bytes,
}

pub fn dependencies_xattrs(
	references: &[tg::Reference],
	max_value_size: usize,
) -> tg::Result<Vec<DependenciesXattr>> {
	if max_value_size == 0 {
		return Err(tg::error!(
			"the maximum xattr value size must be greater than zero"
		));
	}
	let value = serde_json::to_vec(references)
		.map_err(|error| tg::error!(!error, "failed to serialize the dependencies"))?;
	let count = value.len().div_ceil(max_value_size);
	let xattrs = value
		.chunks(max_value_size)
		.enumerate()
		.map(|(index, value)| {
			let name = if count == 1 {
				tg::file::DEPENDENCIES_XATTR_NAME.to_owned()
			} else {
				format!("{}.{index}", tg::file::DEPENDENCIES_XATTR_NAME)
			};
			let value = Bytes::copy_from_slice(value);
			DependenciesXattr { name, value }
		})
		.collect();

	Ok(xattrs)
}

pub fn deserialize_dependencies_xattr(value: &[u8]) -> tg::Result<Vec<tg::Reference>> {
	serde_json::from_slice(value)
		.map_err(|error| tg::error!(!error, "failed to deserialize the dependencies"))
}

#[must_use]
pub fn is_dependencies_xattr_name(name: &str) -> bool {
	if name == tg::file::DEPENDENCIES_XATTR_NAME {
		return true;
	}
	let Some(suffix) = name
		.strip_prefix(tg::file::DEPENDENCIES_XATTR_NAME)
		.and_then(|suffix| suffix.strip_prefix('.'))
	else {
		return false;
	};
	!suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
}

/// Read dependency references from the listed attributes.
pub fn try_read_dependencies_xattrs(
	names: impl IntoIterator<Item = OsString>,
	mut read: impl FnMut(&str) -> std::io::Result<Option<Vec<u8>>>,
) -> tg::Result<Option<Vec<tg::Reference>>> {
	// Order the attributes.
	let mut base = false;
	let mut shards = BTreeMap::new();
	for name in names {
		let Some(name) = name.to_str() else { continue };
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			base = true;
			continue;
		}
		let Some(suffix) = name
			.strip_prefix(tg::file::DEPENDENCIES_XATTR_NAME)
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
	if base {
		if !shards.is_empty() {
			return Err(tg::error!(
				"found both unsharded and sharded dependencies xattrs"
			));
		}
		shards.insert(0, tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
	}
	if shards.is_empty() {
		return Ok(None);
	}

	// Read the references.
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

#[cfg(test)]
mod tests {
	use {super::*, std::path::PathBuf};

	#[test]
	fn json_round_trip() {
		let references = vec![tg::Reference::with_path(PathBuf::from("dependency"))];
		let xattrs = dependencies_xattrs(&references, 4).unwrap();
		assert!(xattrs.len() > 1);
		assert_eq!(xattrs[0].name, "user.tangram.dependencies.0");
		let value = xattrs
			.into_iter()
			.flat_map(|xattr| xattr.value)
			.collect::<Vec<_>>();
		assert_eq!(deserialize_dependencies_xattr(&value).unwrap(), references);
	}

	#[test]
	fn unsharded_round_trip() {
		let references = vec![tg::Reference::with_path(PathBuf::from("dependency"))];
		let xattrs = dependencies_xattrs(&references, usize::MAX).unwrap();
		assert_eq!(xattrs.len(), 1);
		assert_eq!(xattrs[0].name, "user.tangram.dependencies");
		assert_eq!(
			deserialize_dependencies_xattr(&xattrs[0].value).unwrap(),
			references
		);
	}

	#[test]
	fn invalid_shards() {
		for suffixes in [
			vec!["", ".0"],
			vec![".1"],
			vec![".0", ".2"],
			vec![".00"],
			vec![".+0"],
			vec![".invalid"],
			vec!["."],
			vec![".18446744073709551616"],
		] {
			let names = suffixes.into_iter().map(|suffix| {
				OsString::from(format!("{}{suffix}", tg::file::DEPENDENCIES_XATTR_NAME))
			});
			assert!(try_read_dependencies_xattrs(names, |_| Ok(Some(b"[]".to_vec()))).is_err());
		}
	}

	#[test]
	fn unreadable_shards() {
		let names = [OsString::from(tg::file::DEPENDENCIES_XATTR_NAME)];
		assert!(try_read_dependencies_xattrs(names.clone(), |_| Ok(None)).is_err());
		let result = try_read_dependencies_xattrs(names, |_| {
			Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
		});
		assert!(result.is_err());
	}
}
