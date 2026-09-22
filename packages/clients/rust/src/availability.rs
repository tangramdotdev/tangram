use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Availability {
	#[tangram_serialize(id = 0)]
	Object(tg::object::Availability),

	#[tangram_serialize(id = 1)]
	Process(tg::process::Availability),
}

#[cfg(test)]
mod tests {
	use crate::prelude::*;

	#[test]
	fn serialization_preserves_variants() {
		for (availability, kind, variant) in [
			(
				tg::Availability::Object(tg::object::Availability::default()),
				"object",
				0,
			),
			(
				tg::Availability::Process(tg::process::Availability::default()),
				"process",
				1,
			),
		] {
			let json = serde_json::to_value(&availability).unwrap();
			assert_eq!(json, serde_json::json!({"kind": kind, "value": {}}));
			assert_eq!(
				serde_json::from_value::<tg::Availability>(json).unwrap(),
				availability
			);
			let bytes = tangram_serialize::to_vec(&availability).unwrap();
			assert_eq!(bytes, [11, variant, 10, 0]);
			assert_eq!(
				tangram_serialize::from_slice::<tg::Availability>(&bytes).unwrap(),
				availability
			);
		}
	}
}
