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
pub enum Kind {
	#[tangram_serialize(id = 0)]
	Directory,

	#[tangram_serialize(id = 1)]
	File,

	#[tangram_serialize(id = 2)]
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

	// An artifact kind uses a compact numeric tag in Tangram.
	#[test]
	fn serialization() {
		for (kind, expected_id) in [(Kind::Directory, 0), (Kind::File, 1), (Kind::Symlink, 2)] {
			assert_eq!(
				serde_json::to_value(kind).unwrap(),
				serde_json::Value::String(kind.to_string()),
			);
			let bytes = tangram_serialize::to_vec(&kind).unwrap();
			let value = tangram_serialize::from_slice::<tangram_serialize::Value>(&bytes).unwrap();
			let tangram_serialize::Value::Enum(value) = value else {
				panic!("expected an enum");
			};
			assert_eq!(value.id, expected_id);
			let actual = tangram_serialize::from_slice::<Kind>(&bytes).unwrap();
			assert_eq!(actual, kind);
		}
	}
}
