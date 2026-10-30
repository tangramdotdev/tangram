use {
	crate::prelude::*,
	std::collections::BTreeMap,
	tg::interface::{Element, Export, Field, Interface, Language, Literal, Type},
};

/// Get an interface that uses every type.
fn interface() -> Interface {
	let record = Type::Record {
		fields: BTreeMap::from([
			(
				"name".to_owned(),
				Field {
					optional: false,
					type_: Type::String,
				},
			),
			(
				"x-y".to_owned(),
				Field {
					optional: true,
					type_: Type::Map {
						value: Box::new(Type::Number),
					},
				},
			),
		]),
	};
	let objects = [
		Type::Artifact,
		Type::Blob,
		Type::Bytes,
		Type::Directory,
		Type::Error,
		Type::File,
		Type::Graph,
		Type::Module,
		Type::Mutation,
		Type::Object,
		Type::Placeholder,
		Type::Symlink,
		Type::Template,
		Type::Value,
	];
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
						Type::Literal {
							value: Literal::Number(-1.5),
						},
						Type::Literal {
							value: Literal::Boolean(true),
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
	let greet = Export {
		documentation: Some("Greet someone.\n\nLoudly, if asked.".to_owned()),
		error: None,
		range: None,
		type_: Some(Type::Command {
			args: Box::new(args),
			output: Box::new(Type::Reference {
				import: Some("greeting".to_owned()),
				name: "Greeting".to_owned(),
			}),
		}),
	};
	let default = Export {
		documentation: None,
		error: None,
		range: None,
		type_: Some(Type::Command {
			args: Box::new(Type::Tuple {
				elements: Vec::new(),
				rest: Some(Box::new(Type::Value)),
			}),
			output: Box::new(Type::Union { types: Vec::new() }),
		}),
	};
	let version = Export {
		documentation: None,
		error: None,
		range: None,
		type_: Some(Type::Array {
			item: Box::new(Type::Union {
				types: objects.to_vec(),
			}),
		}),
	};
	let broken = Export {
		documentation: None,
		error: Some("the export is a class".to_owned()),
		range: None,
		type_: None,
	};
	let empty = Export {
		documentation: None,
		error: None,
		range: None,
		type_: Some(Type::Tuple {
			elements: Vec::new(),
			rest: None,
		}),
	};
	let greeting = tg::module::Data {
		kind: tg::module::Kind::TypeScript,
		referent: tg::Referent::with_node(tg::module::data::Source::Path("greeting.tg.ts".into())),
	};
	let util = tg::module::Data {
		kind: tg::module::Kind::Python,
		referent: tg::Referent::with_node(tg::module::data::Source::Path("util".into())),
	};
	Interface {
		definitions: BTreeMap::from([
			("Options".to_owned(), record.clone()),
			(
				"Pair".to_owned(),
				Type::Tuple {
					elements: vec![Element {
						optional: false,
						type_: Type::Boolean,
					}],
					rest: None,
				},
			),
			(
				"Unnamed".to_owned(),
				Type::Record {
					fields: BTreeMap::new(),
				},
			),
		]),
		exports: BTreeMap::from([
			("broken".to_owned(), broken),
			("default".to_owned(), default),
			("empty".to_owned(), empty),
			("greet".to_owned(), greet),
			("version".to_owned(), version),
		]),
		imports: BTreeMap::from([("greeting".to_owned(), greeting), ("util".to_owned(), util)]),
	}
}

#[test]
fn an_interface_round_trips_through_every_language() {
	let interface = interface();
	for language in Language::ALL {
		let text = tg::interface::print::print(&interface, language).unwrap();
		let parsed = tg::interface::parse::parse(&text, language).unwrap();
		assert_eq!(parsed, interface, "{text}");
	}
}

#[test]
fn a_handwritten_interface_parses_in_typescript_and_python() {
	let typescript = r#"
		import type * as greeting from "./greeting.tg.ts";

		/** Greet someone by name. */
		export declare const greet: tg.Command<[string, string?], greeting.Greeting>;
		declare const _default: tg.Command<Array<tg.Value>, tg.Directory>;
		export default _default;
		export type Options = { name: string; verbose?: boolean; env: { [key: string]: string }[] };
	"#;
	let python = r#"# /// script
# [tool.tangram.dependencies.greeting]
# specifier = "./greeting.tg.ts"
# ///

greet: tg.Command[tuple[str, NotRequired[str]], greeting.Greeting]
"""Greet someone by name."""

default: tg.Command[list[tg.Value], tg.Directory]

class Options(TypedDict):
    name: str
    verbose: NotRequired[bool]
    env: list[dict[str, str]]
"#;
	let typescript = tg::interface::parse::parse(typescript, Language::TypeScript).unwrap();
	let python = tg::interface::parse::parse(python, Language::Python).unwrap();
	assert_eq!(typescript, python);
	assert_eq!(
		typescript.exports["greet"].documentation.as_deref(),
		Some("Greet someone by name.")
	);
}

#[test]
fn invalid_interfaces_have_ranges() {
	let cases = [
		(
			"\nexport declare const x: Missing;\n",
			Language::TypeScript,
			1,
		),
		(
			"\n\nexport declare const x: tg.Nope;\n",
			Language::TypeScript,
			2,
		),
		("x: str\n\n  y: str\n", Language::Python, 2),
		("{\n  \"exports\": 1\n}\n", Language::Json, 1),
		("declare const x: string;\n", Language::TypeScript, 0),
	];
	for (text, language, line) in cases {
		let Err((_, range)) = tg::interface::parse::parse_with_range(text, language) else {
			panic!("expected an error: {text}");
		};
		assert_eq!(range.unwrap().start.line, line, "{text}");
	}
}
