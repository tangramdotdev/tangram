use {
	super::{Element, Export, Field, Interface, Literal, Type},
	crate::prelude::*,
	std::collections::BTreeMap,
};

#[test]
fn an_interface_round_trips_through_json() {
	let args = Type::Tuple {
		elements: vec![
			Element {
				optional: false,
				type_: Type::String,
			},
			Element {
				optional: true,
				type_: Type::Union {
					types: vec![
						Type::Literal {
							value: Literal::String("loud".to_owned()),
						},
						Type::Null,
					],
				},
			},
		],
		rest: Some(Box::new(Type::Reference {
			name: "Options".to_owned(),
		})),
	};
	let greeting = Export {
		documentation: Some("Greet someone.".to_owned()),
		range: Some(tg::Range {
			end: tg::Position {
				character: 24,
				line: 0,
			},
			start: tg::Position {
				character: 16,
				line: 0,
			},
		}),
		type_: Type::Command {
			args: Box::new(args),
			output: Box::new(Type::File),
		},
	};
	let options = Type::Record {
		fields: BTreeMap::from([
			(
				"count".to_owned(),
				Field {
					optional: true,
					type_: Type::Number,
				},
			),
			(
				"files".to_owned(),
				Field {
					optional: false,
					type_: Type::Map {
						value: Box::new(Type::Artifact),
					},
				},
			),
		]),
	};
	let interface = Interface {
		definitions: BTreeMap::from([("Options".to_owned(), options)]),
		exports: BTreeMap::from([("greeting".to_owned(), greeting)]),
	};
	let json = interface.serialize_json().unwrap();
	assert_eq!(Interface::deserialize_json(&json).unwrap(), interface);
}

#[test]
fn an_interface_uses_the_documented_json_shape() {
	let json = r#"{
		"exports": {
			"greeting": {
				"type": {
					"kind": "command",
					"args": { "kind": "tuple", "elements": [{ "type": { "kind": "string" } }] },
					"output": { "kind": "file" }
				}
			}
		}
	}"#;
	let interface = Interface::deserialize_json(json).unwrap();
	let args = Type::Tuple {
		elements: vec![Element {
			optional: false,
			type_: Type::String,
		}],
		rest: None,
	};
	let greeting = Export {
		documentation: None,
		range: None,
		type_: Type::Command {
			args: Box::new(args),
			output: Box::new(Type::File),
		},
	};
	let expected = Interface {
		definitions: BTreeMap::new(),
		exports: BTreeMap::from([("greeting".to_owned(), greeting)]),
	};
	assert_eq!(interface, expected);
}

#[test]
fn literals_keep_their_json_type() {
	for (json, literal) in [
		("true", Literal::Boolean(true)),
		("1.5", Literal::Number(1.5)),
		(r#""loud""#, Literal::String("loud".to_owned())),
	] {
		assert_eq!(serde_json::from_str::<Literal>(json).unwrap(), literal);
		assert_eq!(serde_json::to_string(&literal).unwrap(), json);
	}
}

#[test]
fn an_interface_rejects_unknown_fields() {
	let json = r#"{ "exports": {}, "imports": {} }"#;
	assert!(Interface::deserialize_json(json).is_err());
	let json = r#"{ "exports": { "x": { "type": { "kind": "number" }, "default": 1 } } }"#;
	assert!(Interface::deserialize_json(json).is_err());
}
