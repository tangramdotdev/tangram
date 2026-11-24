use {std::path::Path, tangram_client::prelude::*};

/// The name of the xattr that records the sandbox to which a directory is assigned.
pub const XATTR_NAME: &str = "user.tangram.sandbox";

const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
	symbols: "0123456789abcdefghjkmnpqrstvwxyz",
};

/// The ID of a sandbox's directory, which the runner generates when it creates the directory, which names the directory, and which stays the same across the sandbox's boots.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(uuid::Uuid);

/// Read the sandbox to which a directory is assigned, or `None` if the directory is not assigned to a sandbox.
pub fn read(path: &Path) -> tg::Result<Option<tg::sandbox::Id>> {
	let Some(value) = xattr::get(path, XATTR_NAME).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to read the sandbox xattr"),
	)?
	else {
		return Ok(None);
	};
	let sandbox = std::str::from_utf8(&value)
		.map_err(|error| tg::error!(!error, path = %path.display(), "invalid sandbox xattr"))?
		.parse()
		.map_err(|error| tg::error!(!error, path = %path.display(), "invalid sandbox xattr"))?;
	Ok(Some(sandbox))
}

/// Assign a directory to a sandbox by recording the sandbox's ID in the directory's xattr.
pub fn write(path: &Path, sandbox: &tg::sandbox::Id) -> tg::Result<()> {
	xattr::set(path, XATTR_NAME, sandbox.to_string().as_bytes()).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to write the sandbox xattr"),
	)?;
	Ok(())
}

impl Id {
	#[must_use]
	pub fn new() -> Self {
		Self(uuid::Uuid::now_v7())
	}
}

impl Default for Id {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", ENCODING.encode(self.0.as_bytes()))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let bytes = ENCODING
			.decode(s.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid directory ID"))?;
		let bytes =
			<[u8; 16]>::try_from(bytes).map_err(|_| tg::error!("invalid directory ID length"))?;
		let id = Self(uuid::Uuid::from_bytes(bytes));
		Ok(id)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn encodes_like_temp_names() {
		let uuid = uuid::Uuid::from_u128(0x0193_0000_0000_7000_8000_0000_0000_0001);
		let id = Id(uuid);
		let string = id.to_string();
		assert_eq!(string.len(), 26);
		assert!(
			string
				.chars()
				.all(|c| "0123456789abcdefghjkmnpqrstvwxyz".contains(c))
		);
		assert_eq!(string.parse::<Id>().unwrap(), id);
	}

	#[test]
	fn orders_by_creation() {
		let first = Id::new();
		let second = Id::new();
		assert!(first < second);
		assert!(first.to_string() < second.to_string());
	}

	#[test]
	fn rejects_invalid_names() {
		let sandbox = tg::sandbox::Id::new();
		assert!(sandbox.to_string().parse::<Id>().is_err());
		assert!("".parse::<Id>().is_err());
		assert!("0123456789abcdefghjkmnpqrs0".parse::<Id>().is_err());
		assert!("0123456789abcdefghjkmnpqri".parse::<Id>().is_err());
	}

	#[test]
	fn xattr_round_trip() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir_all(temp.path()).unwrap();
		assert_eq!(read(temp.path()).unwrap(), None);
		let sandbox = tg::sandbox::Id::new();
		write(temp.path(), &sandbox).unwrap();
		assert_eq!(read(temp.path()).unwrap(), Some(sandbox));
	}

	#[test]
	fn xattr_rejects_an_invalid_value() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir_all(temp.path()).unwrap();
		xattr::set(temp.path(), XATTR_NAME, b"invalid").unwrap();
		assert!(read(temp.path()).is_err());
	}
}
