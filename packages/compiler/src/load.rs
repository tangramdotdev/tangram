#[cfg(not(all(feature = "javascript", feature = "python")))]
use tangram_client::prelude::*;
#[cfg(all(feature = "javascript", feature = "python"))]
use {
	std::{
		collections::{BTreeMap, BTreeSet},
		fmt::Write as _,
	},
	tangram_client::prelude::*,
};

#[cfg(all(feature = "javascript", feature = "python"))]
mod python;
#[cfg(all(feature = "javascript", feature = "python"))]
mod typescript;

/// The prefix of the comment that records the source of each export in bindings.
const MARKER: &str = "tangram: ";

/// The source of an export in bindings, recorded so that positions in the bindings can be mapped to the original module.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Source {
	/// The name of the export.
	pub(crate) name: String,

	/// The range of the export's name in the original module, with UTF-8 character offsets.
	utf8: [u32; 4],

	/// The range of the export's name in the original module, with UTF-16 character offsets.
	utf16: [u32; 4],
}

/// Load a module in a consuming language, generating its bindings from its interface without types if the module is in another language.
#[cfg(all(feature = "javascript", feature = "python"))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	let Some(language) = bindings_language(module, language)? else {
		return Ok(text.to_owned());
	};
	let interface = crate::interface::module(module, text)
		.map_err(|error| tg::error!(!error, "failed to discover the cross-language exports"))?;
	let output = bindings(&interface, text, language);
	Ok(output)
}

/// Get the language of the bindings that a consuming language loads in place of a module, or `None` when the consuming language loads the module as written.
#[cfg(all(feature = "javascript", feature = "python"))]
pub(crate) fn bindings_language(
	module: &tg::module::Data,
	language: Option<tg::module::load::Language>,
) -> tg::Result<Option<tg::module::load::Language>> {
	match (language, module.kind) {
		(Some(_), tg::module::Kind::Config)
		| (Some(tg::module::load::Language::JavaScript), tg::module::Kind::Python)
		| (
			Some(tg::module::load::Language::Python),
			tg::module::Kind::JavaScript | tg::module::Kind::TypeScript,
		) => Ok(language),
		(Some(tg::module::load::Language::Python), tg::module::Kind::TypeScriptDeclaration) => {
			Err(tg::error!("cannot execute a declaration module in python"))
		},
		_ => Ok(None),
	}
}

/// Generate the bindings of an interface in a language, which export a command for each export of the interface.
#[cfg(all(feature = "javascript", feature = "python"))]
#[must_use]
pub fn bindings(
	interface: &tg::Interface,
	text: &str,
	language: tg::module::load::Language,
) -> String {
	match language {
		tg::module::load::Language::JavaScript => typescript_bindings(interface, text),
		tg::module::load::Language::Python => python_bindings(interface, text),
	}
}

#[cfg(not(all(feature = "javascript", feature = "python")))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	if !cfg!(feature = "python")
		&& (module.kind == tg::module::Kind::Python
			|| language == Some(tg::module::load::Language::Python))
	{
		return Err(tg::error!("the python feature is not enabled"));
	}
	if !cfg!(feature = "javascript")
		&& (matches!(
			module.kind,
			tg::module::Kind::JavaScript
				| tg::module::Kind::TypeScript
				| tg::module::Kind::TypeScriptDeclaration
		) || language == Some(tg::module::load::Language::JavaScript))
	{
		return Err(tg::error!("the javascript feature is not enabled"));
	}
	Ok(text.to_owned())
}

/// Get the source of the export that contains an offset in bindings.
pub(crate) fn source(text: &str, offset: usize) -> Option<Source> {
	let text = text.get(..offset)?;
	text.lines().rev().find_map(|line| {
		let json = line
			.strip_prefix("// ")
			.or_else(|| line.strip_prefix("# "))?
			.strip_prefix(MARKER)?;
		serde_json::from_str(json).ok()
	})
}

impl Source {
	#[cfg(all(feature = "javascript", feature = "python"))]
	fn new(name: &str, export: &tg::interface::Export, text: &str) -> Option<Self> {
		let range = export
			.range?
			.try_to_byte_range_in_string(text, tg::position::Encoding::Utf8)?;
		let convert = |encoding| {
			let range = tg::Range::try_from_byte_range_in_string(text, range.clone(), encoding)?;
			Some([
				range.start.line,
				range.start.character,
				range.end.line,
				range.end.character,
			])
		};
		let source = Self {
			name: name.to_owned(),
			utf8: convert(tg::position::Encoding::Utf8)?,
			utf16: convert(tg::position::Encoding::Utf16)?,
		};
		Some(source)
	}

	/// Get the range of the export's name in the original module.
	#[must_use]
	pub(crate) fn range(&self, encoding: tg::position::Encoding) -> tg::Range {
		let [start_line, start_character, end_line, end_character] = match encoding {
			tg::position::Encoding::Utf8 => self.utf8,
			tg::position::Encoding::Utf16 => self.utf16,
		};
		tg::Range {
			end: tg::Position {
				character: end_character,
				line: end_line,
			},
			start: tg::Position {
				character: start_character,
				line: start_line,
			},
		}
	}
}

/// Generate the TypeScript bindings of an interface, which export a command for each export of the interface.
#[cfg(all(feature = "javascript", feature = "python"))]
fn typescript_bindings(interface: &tg::Interface, text: &str) -> String {
	let mut output = String::new();

	// Import the modules whose definitions the types refer to, as namespaces that the compiler resolves from the interface's imports.
	let imports = interface
		.imports
		.keys()
		.enumerate()
		.map(|(index, name)| (name.clone(), format!("$i{index}")))
		.collect::<BTreeMap<_, _>>();
	for (name, namespace) in &imports {
		let quoted = serde_json::to_string(name).unwrap();
		writeln!(output, "import type * as {namespace} from {quoted};").unwrap();
	}

	// Export the interface's named types by their names, so that other bindings can import them.
	let definitions = interface
		.definitions
		.keys()
		.enumerate()
		.map(|(index, name)| {
			let alias = if typescript::is_identifier(name) {
				name.clone()
			} else {
				format!("$t{index}")
			};
			(name.clone(), alias)
		})
		.collect::<BTreeMap<_, _>>();
	let names = typescript::Names {
		definitions,
		imports,
	};
	for (name, type_) in &interface.definitions {
		let alias = &names.definitions[name];
		let type_ = typescript::type_(type_, &names);
		writeln!(output, "export type {alias} = {type_};").unwrap();
	}

	for (index, (name, export)) in interface.exports.iter().enumerate() {
		// Define a function whose name is the export's name, so that a command built from it targets the export.
		let quoted = serde_json::to_string(name).unwrap();
		writeln!(
			output,
			"const f{index} = {{ async {quoted}() {{}} }}[{quoted}];"
		)
		.unwrap();

		// Record the source of the export and its documentation.
		if let Some(source) = Source::new(name, export, text) {
			writeln!(
				output,
				"// {MARKER}{}",
				serde_json::to_string(&source).unwrap()
			)
			.unwrap();
		}
		if let Some(documentation) = &export.documentation {
			output.push_str("/**\n");
			for line in documentation.replace("*/", "*\\/").lines() {
				if line.is_empty() {
					output.push_str(" *\n");
				} else {
					writeln!(output, " * {line}").unwrap();
				}
			}
			output.push_str(" */\n");
		}

		// Export the command.
		let type_ = typescript::export(export, &names);
		writeln!(
			output,
			"const c{index}: {type_} = tg.command(f{index}) as unknown as {type_};\nexport {{ c{index} as {quoted} }};"
		)
		.unwrap();
	}
	output.push_str("export {};\n");

	output
}

/// Generate the Python bindings of an interface, which bind a command to each export of the interface that Python can name.
#[cfg(all(feature = "javascript", feature = "python"))]
fn python_bindings(interface: &tg::Interface, text: &str) -> String {
	let mut output = String::new();

	// Bind each export that Python can name.
	let names = interface.exports.keys().cloned().collect();
	let bindings = interface
		.exports
		.iter()
		.filter_map(|(name, export)| {
			let binding = python::binding(name, &names).ok()?;
			Some((binding, (name, export)))
		})
		.collect::<BTreeMap<_, _>>();

	// Choose helper names that cannot shadow a binding or a definition, preferring the client's global name.
	let mut prefix = "_tg".to_owned();
	while bindings
		.keys()
		.chain(interface.definitions.keys())
		.any(|name| name.starts_with(&prefix))
	{
		prefix.push('_');
	}

	// Declare the modules whose definitions the types refer to as dependencies, which the compiler resolves from the interface's imports.
	let imports = interface
		.imports
		.keys()
		.enumerate()
		.map(|(index, name)| (name.clone(), format!("{prefix}_import_{index}")))
		.collect::<BTreeMap<_, _>>();
	if !imports.is_empty() {
		output.push_str("# /// script\n");
		for (name, module) in &imports {
			let quoted = serde_json::to_string(name).unwrap();
			writeln!(output, "# [tool.tangram.dependencies.{module}]").unwrap();
			writeln!(output, "# specifier = {quoted}").unwrap();
		}
		output.push_str("# ///\n");
	}
	output.push_str("from __future__ import annotations\n");
	let typing = format!("{prefix}_typing");
	writeln!(output, "import typing as {typing}").unwrap();
	let mut references = BTreeSet::new();
	for type_ in interface.definitions.values().chain(
		interface
			.exports
			.values()
			.filter_map(|export| export.type_.as_ref()),
	) {
		python::references(type_, &mut references);
	}
	if !references.is_empty() {
		writeln!(output, "if {typing}.TYPE_CHECKING:").unwrap();
		for (import, name) in &references {
			let Some(module) = imports.get(import) else {
				continue;
			};
			writeln!(output, "    from {module} import {name} as {module}_{name}").unwrap();
		}
	}
	let client = if bindings.contains_key("tg") {
		writeln!(output, "import tangram as {prefix}_client").unwrap();
		format!("{prefix}_client")
	} else {
		"tg".to_owned()
	};

	// Name the interface's definitions by their names, so that other bindings can import them, unless a name is not a Python identifier or a binding uses it.
	let definitions = interface
		.definitions
		.keys()
		.enumerate()
		.map(|(index, name)| {
			let alias = if python::is_identifier(name) && !bindings.contains_key(name) {
				name.clone()
			} else {
				format!("{prefix}_type_{index}")
			};
			(name.clone(), alias)
		})
		.collect::<BTreeMap<_, _>>();
	let names = python::Names {
		client: client.clone(),
		definitions,
		imports,
		prefix: prefix.clone(),
		records: std::cell::RefCell::default(),
		typing: typing.clone(),
	};

	// Print the definitions and the types of the exports.
	let definitions = interface
		.definitions
		.iter()
		.map(|(name, type_)| python::definition(&names.definitions[name], type_, &names))
		.collect::<Vec<_>>();
	let types = bindings
		.values()
		.map(|(_, export)| python::export(export, &names))
		.collect::<Vec<_>>();
	for record in names.records.borrow().iter() {
		writeln!(output, "{record}").unwrap();
	}
	for definition in &definitions {
		writeln!(output, "{definition}").unwrap();
	}

	for (index, ((binding, (name, export)), type_)) in bindings.iter().zip(types).enumerate() {
		// Define a function that records the export's name, so that a command built from it targets the export.
		let function = format!("{prefix}_function_{index}");
		writeln!(output, "async def {function}(): pass").unwrap();
		let quoted = serde_json::to_string(name).unwrap();
		writeln!(output, "{function}.__tangram_export__ = {quoted}").unwrap();

		// Record the source of the export.
		if let Some(source) = Source::new(name, export, text) {
			writeln!(
				output,
				"# {MARKER}{}",
				serde_json::to_string(&source).unwrap()
			)
			.unwrap();
		}

		// Bind the command, followed by the export's documentation.
		let quoted_type = serde_json::to_string(&type_).unwrap();
		writeln!(
			output,
			"{binding} = {typing}.cast({quoted_type}, {client}.command({function}))"
		)
		.unwrap();
		if let Some(documentation) = &export.documentation {
			writeln!(output, "{}", serde_json::to_string(documentation).unwrap()).unwrap();
		}
	}

	// List the bindings and the definitions that keep their names as the module's public names.
	let names = bindings
		.keys()
		.chain(
			names
				.definitions
				.iter()
				.filter(|(name, alias)| name == alias)
				.map(|(name, _)| name),
		)
		.collect::<BTreeSet<_>>();
	let names = serde_json::to_string(&names).unwrap();
	writeln!(output, "__all__ = {names}").unwrap();

	output
}

#[cfg(all(test, feature = "javascript", feature = "python"))]
mod tests {
	use super::*;

	pub(super) fn source(kind: tg::module::Kind) -> tg::module::Data {
		tg::module::Data {
			kind,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/test/module".into(),
			)),
		}
	}

	#[test]
	fn preserves_original_source_without_translation() {
		let source = source(tg::module::Kind::Python);
		let text = "def default(): return 42\n";
		assert_eq!(module(&source, text, None).unwrap(), text);
		assert_eq!(
			module(&source, text, Some(tg::module::load::Language::Python)).unwrap(),
			text
		);
	}

	#[test]
	fn generates_parseable_bindings_in_both_languages() {
		let source = source(tg::module::Kind::Python);
		let text = module(
			&source,
			"def default(): return 42\ndef f0(): pass\n",
			Some(tg::module::load::Language::JavaScript),
		)
		.unwrap();
		let result = crate::Compiler::transpile(&text, &source);
		assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
		let mut source = source;
		source.kind = tg::module::Kind::TypeScript;
		let text = module(
			&source,
			"export default () => 42; export const _tg_client = () => 1;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(!text.contains("import tangram as _tg_client\n"));
		assert!(text.contains("\n_tg_client = "));
		assert!(text.contains("\ndefault = "));
	}

	#[test]
	fn records_the_sources_of_exports() {
		// A JavaScript representation of a Python module records each export's source and docstring.
		let source_ = source(tg::module::Kind::Python);
		let python = "x = 1\n\ndef greet(name):\n    \"\"\"Greet someone.\n\n    Args:\n        name: The name.\n    \"\"\"\n";
		let text = module(
			&source_,
			python,
			Some(tg::module::load::Language::JavaScript),
		)
		.unwrap();
		assert!(
			text.contains("/**\n * Greet someone.\n *\n * Args:\n *     name: The name.\n */\n"),
			"{text}"
		);
		let offset = text.find("export { c0 as \"greet\" }").unwrap();
		let export = super::source(&text, offset).unwrap();
		assert_eq!(export.name, "greet");
		let range = export.range(tg::position::Encoding::Utf8);
		assert_eq!((range.start.line, range.start.character), (2, 4));
		assert_eq!((range.end.line, range.end.character), (2, 9));

		// A Python representation of a TypeScript module records each export's source and documentation.
		let mut source_ = source_;
		source_.kind = tg::module::Kind::TypeScript;
		let typescript =
			"/** Say hello. */\nexport const hello = () => \"hi\";\nexport const other = 1;";
		let text = module(
			&source_,
			typescript,
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(
			text.contains(
				"hello = _tg_typing.cast(\"tg.Command[tuple[tg.Value.Type, ...], tg.Value.Type]\", tg.command(_tg_function_0))\n\"Say hello.\"\n"
			),
			"{text}"
		);
		let offset = text.find("\nhello = ").unwrap() + 1;
		let export = super::source(&text, offset).unwrap();
		assert_eq!(export.name, "hello");
		let range = export.range(tg::position::Encoding::Utf16);
		assert_eq!((range.start.line, range.start.character), (1, 13));
		assert!(super::source(&text, 0).is_none());
	}

	#[test]
	fn renames_python_keyword_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const lambda = () => 42; export const match = () => 42; export const type = () => 42; export const _ = () => 42; export default () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(text.contains("\nlambda_ = "), "{text}");
		// Python writes a soft keyword and the default export's name as attributes, so they keep their names.
		for name in ["match", "type", "_", "default"] {
			assert!(text.contains(&format!("\n{name} = ")), "{text}");
		}
		assert!(text.contains("__all__ = [\"_\",\"default\",\"lambda_\",\"match\",\"type\"]\n"));
	}

	#[test]
	fn renames_colliding_python_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const lambda = () => 42; export const lambda_ = () => 42; export const lambda__ = () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		// The export's own name takes precedence over the escaped name of another export, and each binding's command targets its export.
		assert!(
			text.contains("_tg_function_0.__tangram_export__ = \"lambda_\"\n"),
			"{text}"
		);
		assert!(text.contains("\nlambda_ = _tg_typing.cast("), "{text}");
		assert!(
			text.contains("_tg_function_2.__tangram_export__ = \"lambda\"\n"),
			"{text}"
		);
		assert!(text.contains("\nlambda___ = _tg_typing.cast(\"tg.Command[tuple[tg.Value.Type, ...], tg.Value.Type]\", tg.command(_tg_function_2))"), "{text}");
		assert!(text.contains("__all__ = [\"lambda_\",\"lambda__\",\"lambda___\"]\n"));
	}

	#[test]
	fn omits_unbindable_python_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const $ = () => 42; export const __all__ = () => 42; export const value = () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(!text.contains('$'), "{text}");
		assert!(!text.contains("\n__all__ = _tg"), "{text}");
		assert!(text.contains("__all__ = [\"value\"]\n"), "{text}");
	}

	#[test]
	fn generates_typed_commands() {
		let args = tg::interface::Type::Tuple {
			elements: vec![
				tg::interface::Element {
					optional: false,
					type_: tg::interface::Type::String,
				},
				tg::interface::Element {
					optional: true,
					type_: tg::interface::Type::Reference {
						import: None,
						name: "Options".to_owned(),
					},
				},
			],
			rest: None,
		};
		let greeting = tg::interface::Export {
			documentation: None,
			error: None,
			range: None,
			type_: Some(tg::interface::Type::Command {
				args: Box::new(args),
				output: Box::new(tg::interface::Type::File),
			}),
		};
		let field = tg::interface::Field {
			optional: true,
			type_: tg::interface::Type::Boolean,
		};
		let options = tg::interface::Type::Record {
			fields: [("loud".to_owned(), field)].into(),
		};
		let interface = tg::Interface {
			definitions: [("Options".to_owned(), options)].into(),
			exports: [("greeting".to_owned(), greeting)].into(),
			imports: BTreeMap::new(),
		};

		// The TypeScript module declares the command's type.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("export type Options = { \"loud\"?: boolean };\n"),
			"{text}"
		);
		assert!(
			text.contains(
				"const c0: tg.Command<([string] | [string, Options]), tg.File> = tg.command(f0)"
			),
			"{text}"
		);
		let source = source(tg::module::Kind::Python);
		let result = crate::Compiler::transpile(&text, &source);
		assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);

		// The Python module declares the command's type.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.contains("Options = _tg_typing.TypedDict(\"Options\", {\"loud\": \"_tg_typing.NotRequired[bool]\"}, closed=True)\n"),
			"{text}"
		);
		assert!(
			text.contains("greeting = _tg_typing.cast(\"tg.Command[tuple[str, *tuple[Options, ...]], tg.File]\", tg.command(_tg_function_0))\n"),
			"{text}"
		);
	}

	#[test]
	fn types_an_export_that_is_not_a_value_with_its_reason() {
		let curry = tg::interface::Export {
			documentation: None,
			error: Some("a function is not a value".to_owned()),
			range: None,
			type_: None,
		};
		let interface = tg::Interface {
			definitions: BTreeMap::new(),
			exports: [("curry".to_owned(), curry)].into(),
			imports: BTreeMap::new(),
		};

		// The TypeScript module types the export with the reason.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("const c0: tg.NonValue<\"a function is not a value\"> = tg.command(f0)"),
			"{text}"
		);

		// The Python module types the export with the reason.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.contains("curry = _tg_typing.cast(\"tg.NonValue[_tg_typing.Literal['a function is not a value']]\", tg.command(_tg_function_0))\n"),
			"{text}"
		);
	}

	#[test]
	fn imports_the_modules_that_references_name() {
		let module = source(tg::module::Kind::TypeScript);
		let greeting = tg::interface::Export {
			documentation: None,
			error: None,
			range: None,
			type_: Some(tg::interface::Type::Command {
				args: Box::new(tg::interface::Type::Tuple {
					elements: Vec::new(),
					rest: None,
				}),
				output: Box::new(tg::interface::Type::Reference {
					import: Some("greeting".to_owned()),
					name: "Greeting".to_owned(),
				}),
			}),
		};
		let interface = tg::Interface {
			definitions: BTreeMap::new(),
			exports: [("greet".to_owned(), greeting)].into(),
			imports: [("greeting".to_owned(), module)].into(),
		};

		// The TypeScript module imports the referenced module as a namespace.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("import type * as $i0 from \"greeting\";\n"),
			"{text}"
		);
		assert!(
			text.contains("const c0: tg.Command<[], $i0.Greeting> = tg.command(f0)"),
			"{text}"
		);

		// The Python module declares the referenced module as a dependency and imports it while type checking.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.starts_with(
				"# /// script\n# [tool.tangram.dependencies._tg_import_0]\n# specifier = \"greeting\"\n# ///\n"
			),
			"{text}"
		);
		assert!(
			text.contains(
				"if _tg_typing.TYPE_CHECKING:\n    from _tg_import_0 import Greeting as _tg_import_0_Greeting\n"
			),
			"{text}"
		);
		assert!(
			text.contains(
				"greet = _tg_typing.cast(\"tg.Command[tuple[()], _tg_import_0_Greeting]\""
			),
			"{text}"
		);
	}
}
