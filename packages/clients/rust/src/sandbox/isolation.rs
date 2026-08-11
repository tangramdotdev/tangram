use crate::prelude::*;

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "kind")]
#[tangram_serialize(display, from_str)]
pub enum Isolation {
	Container,

	Seatbelt,

	Vm,
}

impl std::str::FromStr for Isolation {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		match value {
			"container" => Ok(Self::Container),
			"seatbelt" => Ok(Self::Seatbelt),
			"vm" => Ok(Self::Vm),
			_ => Err(tg::error!(%value, "invalid isolation")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Isolation uses its canonical string representation in Tangram.
	#[test]
	fn serialization() {
		let isolation = Isolation::Container;
		assert_eq!(
			serde_json::to_value(isolation).unwrap(),
			serde_json::json!({ "kind": "container" }),
		);
		let string = isolation.to_string();
		let bytes = tangram_serialize::to_vec(&isolation).unwrap();
		assert_eq!(bytes, tangram_serialize::to_vec(&string).unwrap());
		let actual = tangram_serialize::from_slice::<Isolation>(&bytes).unwrap();
		assert_eq!(actual, isolation);
	}
}
