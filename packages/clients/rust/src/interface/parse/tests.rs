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
		reexport: None,
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
		reexport: None,
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
		reexport: None,
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
		reexport: None,
		type_: None,
	};
	let empty = Export {
		documentation: None,
		error: None,
		range: None,
		reexport: None,
		type_: Some(Type::Tuple {
			elements: Vec::new(),
			rest: None,
		}),
	};
	let import = tg::module::Import::with_specifier_and_attributes("./tools.tg.ts", None).unwrap();
	let tools = Export {
		documentation: Some("The tools.".to_owned()),
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: None,
			import,
		}),
		type_: None,
	};
	let attributes = BTreeMap::from([("kind".to_owned(), "python".to_owned())]);
	let import =
		tg::module::Import::with_specifier_and_attributes("./util", Some(attributes)).unwrap();
	let util_namespace = Export {
		documentation: None,
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: None,
			import,
		}),
		type_: None,
	};
	let import = tg::module::Import::with_specifier_and_attributes("./barrel.tg.ts", None).unwrap();
	let forwarded = Export {
		documentation: Some("The forwarded tools.".to_owned()),
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: Some("tools".to_owned()),
			import,
		}),
		type_: None,
	};
	let import = tg::module::Import::with_specifier_and_attributes("./tools.tg.ts", None).unwrap();
	let tool = Export {
		documentation: None,
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: Some("tool".to_owned()),
			import,
		}),
		type_: None,
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
			("forwarded".to_owned(), forwarded),
			("greet".to_owned(), greet),
			("tool".to_owned(), tool),
			("tools".to_owned(), tools),
			("util".to_owned(), util_namespace),
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
		let parsed = without_ranges(tg::interface::parse::parse(&text, language).unwrap());
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

		/** The tools. */
		export * as tools from "./tools.tg.ts";

		/** The tool. */
		export { tool, default as main } from "./tools.tg.ts";
	"#;
	let python = r#"# /// script
# [tool.tangram.dependencies.greeting]
# specifier = "./greeting.tg.ts"
# [tool.tangram.dependencies.tools]
# specifier = "./tools.tg.ts"
# ///

greet: tg.Command[tuple[str, NotRequired[str]], greeting.Greeting]
"""Greet someone by name."""

default: tg.Command[list[tg.Value], tg.Directory]

class Options(TypedDict):
    name: str
    verbose: NotRequired[bool]
    env: list[dict[str, str]]

import tools
"""The tools."""

from tools import tool, default as main
"""The tool."""
"#;
	let typescript = tg::interface::parse::parse(typescript, Language::TypeScript).unwrap();
	let python = tg::interface::parse::parse(python, Language::Python).unwrap();
	assert_eq!(
		typescript.exports["greet"].range.map(|range| range.start),
		Some(tg::Position {
			character: 23,
			line: 4,
		})
	);
	let typescript = without_ranges(typescript);
	let python = without_ranges(python);
	assert_eq!(typescript, python);
	assert_eq!(
		typescript.exports["greet"].documentation.as_deref(),
		Some("Greet someone by name.")
	);
	let main = typescript.exports["main"].reexport.as_ref().unwrap();
	assert_eq!(main.export.as_deref(), Some("default"));
	assert_eq!(main.import.reference.to_string(), "./tools.tg.ts");
	assert_eq!(
		typescript.exports["main"].documentation.as_deref(),
		Some("The tool.")
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
		("x: str\nimport missing\n", Language::Python, 1),
		("x: str\nfrom missing import x\n", Language::Python, 1),
		("\nexport { \"x\" };\n", Language::TypeScript, 1),
	];
	for (text, language, line) in cases {
		let Err((_, range)) = tg::interface::parse::parse_with_range(text, language) else {
			panic!("expected an error: {text}");
		};
		assert_eq!(range.unwrap().start.line, line, "{text}");
	}
}

#[test]
fn reexports_whose_names_are_not_identifiers_round_trip_through_typescript() {
	let import = tg::module::Import::with_specifier_and_attributes("./tools.tg.ts", None).unwrap();
	let reexport = |export: Option<&str>| Export {
		documentation: None,
		error: None,
		range: None,
		reexport: Some(tg::interface::Reexport {
			export: export.map(ToOwned::to_owned),
			import: import.clone(),
		}),
		type_: None,
	};
	let interface = Interface {
		definitions: BTreeMap::new(),
		exports: BTreeMap::from([
			("a-b".to_owned(), reexport(Some("x-y"))),
			("x-y".to_owned(), reexport(None)),
			("y-z".to_owned(), reexport(Some("y-z"))),
		]),
		imports: BTreeMap::new(),
	};
	let text = tg::interface::print::print(&interface, Language::TypeScript).unwrap();
	for statement in [
		r#"export { "x-y" as "a-b" } from "./tools.tg.ts";"#,
		r#"export * as "x-y" from "./tools.tg.ts";"#,
		r#"export { "y-z" } from "./tools.tg.ts";"#,
	] {
		assert!(text.contains(statement), "{text}");
	}
	let parsed = without_ranges(tg::interface::parse::parse(&text, Language::TypeScript).unwrap());
	assert_eq!(parsed, interface);
	assert!(tg::interface::print::print(&interface, Language::Python).is_err());
}

/// Remove the ranges of an interface's exports, which depend on the text that it was parsed from.
fn without_ranges(mut interface: Interface) -> Interface {
	for export in interface.exports.values_mut() {
		export.range = None;
	}
	interface
}
