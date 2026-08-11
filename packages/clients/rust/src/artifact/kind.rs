use crate::prelude::*;

/// An artifact kind.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Kind {
	Directory,

	File,

	Symlink,
}

impl From<Kind> for tg::object::Kind {
	fn from(value: Kind) -> Self {
		match value {
			Kind::Directory => tg::object::Kind::Directory,
			Kind::File => tg::object::Kind::File,
			Kind::Symlink => tg::object::Kind::Symlink,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// An artifact kind has the same canonical string representation in JSON and Tangram.
	#[test]
	fn serialization() {
		for kind in [Kind::Directory, Kind::File, Kind::Symlink] {
			let string = kind.to_string();
			assert_eq!(
				serde_json::to_value(kind).unwrap(),
				serde_json::Value::String(string.clone()),
			);
			assert_eq!(
				tangram_serialize::to_vec(&kind).unwrap(),
				tangram_serialize::to_vec(&string).unwrap(),
			);
			let bytes = tangram_serialize::to_vec(&kind).unwrap();
			let actual = tangram_serialize::from_slice::<Kind>(&bytes).unwrap();
			assert_eq!(actual, kind);
		}
	}
}
