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
			import: None,
			name: "Options".to_owned(),
		})),
	};
	let greeting = Export {
		documentation: Some("Greet someone.".to_owned()),
		error: None,
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
		reexport: None,
		type_: Some(Type::Command {
			args: Box::new(args),
			output: Box::new(Type::File),
		}),
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
		imports: BTreeMap::new(),
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
		error: None,
		range: None,
		reexport: None,
		type_: Some(Type::Command {
			args: Box::new(args),
			output: Box::new(Type::File),
		}),
	};
	let expected = Interface {
		definitions: BTreeMap::new(),
		exports: BTreeMap::from([("greeting".to_owned(), greeting)]),
		imports: BTreeMap::new(),
	};
	assert_eq!(interface, expected);
}

#[test]
fn an_export_that_is_not_a_value_has_an_error() {
	let json = r#"{ "exports": { "curry": { "error": "a function is not a value" } } }"#;
	let interface = Interface::deserialize_json(json).unwrap();
	let curry = Export {
		documentation: None,
		error: Some("a function is not a value".to_owned()),
		range: None,
		reexport: None,
		type_: None,
	};
	let expected = Interface {
		definitions: BTreeMap::new(),
		exports: BTreeMap::from([("curry".to_owned(), curry)]),
		imports: BTreeMap::new(),
	};
	assert_eq!(interface, expected);
	assert_eq!(
		Interface::deserialize_json(&interface.serialize_json().unwrap()).unwrap(),
		interface
	);
}

#[test]
fn a_reexport_has_the_import_of_its_module_and_the_name_of_its_export() {
	let json = r#"{ "exports": { "tool": { "reexport": { "export": "tool", "import": { "reference": "./tools.tg.ts" } } }, "tools": { "reexport": { "import": { "reference": "./tools.tg.ts" } } } } }"#;
	let interface = Interface::deserialize_json(json).unwrap();
	let import = tg::module::Import::with_specifier_and_attributes("./tools.tg.ts", None).unwrap();
	let tool = Export {
		documentation: None,
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: Some("tool".to_owned()),
			import: import.clone(),
		}),
		type_: None,
	};
	let tools = Export {
		documentation: None,
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: None,
			import,
		}),
		type_: None,
	};
	let expected = Interface {
		definitions: BTreeMap::new(),
		exports: BTreeMap::from([("tool".to_owned(), tool), ("tools".to_owned(), tools)]),
		imports: BTreeMap::new(),
	};
	assert_eq!(interface, expected);
	assert_eq!(
		Interface::deserialize_json(&interface.serialize_json().unwrap()).unwrap(),
		interface
	);
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
	let json = r#"{ "exports": {}, "types": {} }"#;
	assert!(Interface::deserialize_json(json).is_err());
	let json = r#"{ "exports": { "x": { "type": { "kind": "number" }, "default": 1 } } }"#;
	assert!(Interface::deserialize_json(json).is_err());
}
