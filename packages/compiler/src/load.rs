#[cfg(not(all(feature = "javascript", feature = "python")))]
use tangram_client::prelude::*;
#[cfg(all(feature = "javascript", feature = "python"))]
use {
	std::{collections::BTreeMap, fmt::Write as _},
	tangram_client::prelude::*,
};

#[cfg(all(feature = "javascript", feature = "python"))]
mod python;

/// The prefix of the comment that records the source of each export in a generated module.
const MARKER: &str = "tangram: ";

/// The source of an export in a generated module, recorded so that positions in the generated module can be mapped to the original module.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Source {
	/// The name of the export.
	pub(crate) name: String,

	/// The range of the export's name in the original module, with UTF-8 character offsets.
	utf8: [u32; 4],

	/// The range of the export's name in the original module, with UTF-16 character offsets.
	utf16: [u32; 4],
}

/// Generate the consuming language's representation without changing the module's identity.
#[cfg(all(feature = "javascript", feature = "python"))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	match (language, module.kind) {
		(Some(tg::module::load::Language::JavaScript), tg::module::Kind::Python)
		| (
			Some(tg::module::load::Language::Python),
			tg::module::Kind::JavaScript | tg::module::Kind::TypeScript,
		) => (),
		(Some(tg::module::load::Language::Python), tg::module::Kind::TypeScriptDeclaration) => {
			return Err(tg::error!("cannot execute a declaration module in python"));
		},
		_ => return Ok(text.to_owned()),
	}
	let interface = crate::interface::module(module, text)
		.map_err(|error| tg::error!(!error, "failed to discover the cross-language exports"))?;
	let exports = interface.exports;

	let mut output = String::new();
	match language.unwrap() {
		tg::module::load::Language::JavaScript => {
			for (index, (name, export)) in exports.iter().enumerate() {
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

				let name = serde_json::to_string(name).unwrap();
				writeln!(output, "const f{index} = {{\nasync {name}(...args: Array<tg.Value>): Promise<tg.Value> {{ return await tg.command(f{index}, ...args).build(); }}\n}}[{name}];\nexport {{ f{index} as {name} }};").unwrap();
			}
			output.push_str("export {};\n");
		},
		tg::module::load::Language::Python => {
			// Bind each export that Python can name.
			let names = exports.keys().cloned().collect();
			let bindings = exports
				.iter()
				.filter_map(|(name, export)| {
					let binding = python::binding(name, &names).ok()?;
					Some((binding, (name, export)))
				})
				.collect::<BTreeMap<_, _>>();

			// Choose helper names that cannot shadow a binding, preferring the client's global name.
			let mut prefix = "_tg".to_owned();
			while bindings.keys().any(|binding| binding.starts_with(&prefix)) {
				prefix.push('_');
			}
			let client = if bindings.contains_key("tg") {
				writeln!(output, "import tangram as {prefix}_client").unwrap();
				format!("{prefix}_client")
			} else {
				"tg".to_owned()
			};
			let arguments = if bindings.contains_key("args") {
				format!("{prefix}_args")
			} else {
				"args".to_owned()
			};

			// Generate a function for each binding, recording the source of the export and its documentation.
			for (binding, (name, export)) in &bindings {
				if let Some(source) = Source::new(name, export, text) {
					writeln!(
						output,
						"# {MARKER}{}",
						serde_json::to_string(&source).unwrap()
					)
					.unwrap();
				}
				writeln!(
					output,
					"async def {binding}(*{arguments}: {client}.Value.Type) -> {client}.Value.Type:"
				)
				.unwrap();
				if let Some(documentation) = &export.documentation {
					writeln!(
						output,
						"    {}",
						serde_json::to_string(documentation).unwrap()
					)
					.unwrap();
				}
				writeln!(
					output,
					"    return await {client}.command({binding}, *{arguments}).build()"
				)
				.unwrap();
				// Record the export name so that the function targets the export rather than the binding.
				if binding != *name {
					let name = serde_json::to_string(name).unwrap();
					writeln!(output, "{binding}.__tangram_export__ = {name}").unwrap();
				}
			}

			let names = bindings.keys().collect::<Vec<_>>();
			let names = serde_json::to_string(&names).unwrap();
			writeln!(output, "__all__ = {names}").unwrap();
		},
	}
	Ok(output)
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

/// Get the source of the export that contains an offset in a generated module.
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
	fn generates_parseable_modules_in_both_languages() {
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
		assert!(text.contains("async def _tg_client("));
		assert!(text.contains("async def default("));
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
		let offset = text.find("async \"greet\"").unwrap();
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
				"async def hello(*args: tg.Value.Type) -> tg.Value.Type:\n    \"Say hello.\"\n"
			),
			"{text}"
		);
		let offset = text.find("async def hello").unwrap();
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
		assert!(text.contains("async def lambda_("));
		assert!(text.contains("lambda_.__tangram_export__ = \"lambda\"\n"));
		// Python writes a soft keyword and the default export's name as attributes, so they keep their names.
		for name in ["match", "type", "_", "default"] {
			assert!(text.contains(&format!("async def {name}(")), "{text}");
			assert!(
				!text.contains(&format!("\n{name}.__tangram_export__")),
				"{text}"
			);
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
		// The export's own name takes precedence over the escaped name of another export.
		assert!(text.contains("async def lambda_("));
		assert!(!text.contains("lambda_.__tangram_export__"));
		assert!(text.contains("async def lambda__("));
		assert!(text.contains("async def lambda___("));
		assert!(text.contains("lambda___.__tangram_export__ = \"lambda\"\n"));
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
		assert!(!text.contains("def __all__("), "{text}");
		assert!(text.contains("__all__ = [\"value\"]\n"), "{text}");
	}
}
