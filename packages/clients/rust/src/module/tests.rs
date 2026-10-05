use crate::module::{Kind, load::Language};

#[test]
fn language_names_preserve_numeric_ids() {
	for (kind, name, id) in [
		(Kind::JavaScript, "javascript", 0u8),
		(Kind::TypeScript, "typescript", 1),
		(Kind::TypeScriptDeclaration, "typescript_declaration", 2),
		(Kind::Python, "python", 12),
	] {
		assert_eq!(kind.to_string(), name);
		assert_eq!(name.parse::<Kind>().unwrap(), kind);
		assert_eq!(serde_json::to_value(kind).unwrap(), name);
		assert_eq!(
			serde_json::from_value::<Kind>(serde_json::json!(name)).unwrap(),
			kind
		);
		let bytes = tangram_serialize::to_vec(&kind).unwrap();
		assert_eq!(tangram_serialize::from_slice::<Kind>(&bytes).unwrap(), kind);
		assert_eq!(
			bytes,
			tangram_serialize::to_vec(&tangram_serialize::Value::Enum(
				tangram_serialize::value::Enum {
					id,
					value: Box::new(tangram_serialize::Value::Null)
				}
			))
			.unwrap()
		);
	}
}

#[test]
fn load_language_names_round_trip() {
	for (language, name) in [
		(Language::JavaScript, "javascript"),
		(Language::Python, "python"),
	] {
		assert_eq!(serde_json::to_value(language).unwrap(), name);
		assert_eq!(
			serde_json::from_value::<Language>(serde_json::json!(name)).unwrap(),
			language
		);
	}
}
