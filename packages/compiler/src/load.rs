#[cfg(not(feature = "python"))]
use tangram_client::prelude::*;
#[cfg(feature = "python")]
use {
	std::{collections::BTreeMap, fmt::Write as _},
	tangram_client::prelude::*,
};

#[cfg(feature = "python")]
mod javascript;
#[cfg(feature = "python")]
mod python;

/// Generate the consuming language's representation without changing the module's identity.
#[cfg(feature = "python")]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	let exports = match (language, module.kind) {
		(Some(tg::module::load::Language::JavaScript), tg::module::Kind::Python) => {
			python::exports(module, text)
		},
		(
			Some(tg::module::load::Language::Python),
			tg::module::Kind::JavaScript | tg::module::Kind::TypeScript,
		) => javascript::exports(module, text),
		(Some(tg::module::load::Language::Python), tg::module::Kind::TypeScriptDeclaration) => {
			return Err(tg::error!("cannot execute a declaration module in python"));
		},
		_ => return Ok(text.to_owned()),
	};
	let exports = exports.map_err(|error| tg::error!(!error, module = ?module.without_token(), "failed to discover the cross-language exports"))?;

	let mut output = String::new();
	match language.unwrap() {
		tg::module::load::Language::JavaScript => {
			for (index, name) in exports.iter().enumerate() {
				let name = serde_json::to_string(name).unwrap();
				writeln!(output, "const f{index} = {{\nasync {name}(...args: Array<tg.Value>): Promise<tg.Value> {{ return await tg.command(f{index}, ...args).build(); }}\n}}[{name}];\nexport {{ f{index} as {name} }};").unwrap();
			}
			output.push_str("export {};\n");
		},
		tg::module::load::Language::Python => {
			// Bind each export that Python can name.
			let bindings = exports
				.iter()
				.filter_map(|name| {
					let binding = python::binding(name, &exports).ok()?;
					Some((binding, name))
				})
				.collect::<BTreeMap<_, _>>();

			// Choose helper names that cannot shadow a binding.
			let mut prefix = "_tg".to_owned();
			while bindings.keys().any(|binding| binding.starts_with(&prefix)) {
				prefix.push('_');
			}

			// Generate a function for each binding.
			writeln!(output, "import tangram as {prefix}_client").unwrap();
			for (binding, name) in &bindings {
				writeln!(
					output,
					"async def {binding}(*{prefix}_args: {prefix}_client.Value.Type) -> {prefix}_client.Value.Type:\n    return await {prefix}_client.command({binding}, *{prefix}_args).build()"
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

#[cfg(not(feature = "python"))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	if module.kind == tg::module::Kind::Python
		|| language == Some(tg::module::load::Language::Python)
	{
		return Err(tg::error!("the python feature is not enabled"));
	}
	Ok(text.to_owned())
}

#[cfg(feature = "python")]
fn located_error(
	module: &tg::module::Data,
	text: &str,
	range: std::ops::Range<usize>,
	error: tg::Error,
) -> tg::Error {
	let Ok(module) = tg::Module::try_from_data(module.without_token()) else {
		return error;
	};
	let Some(range) =
		tg::Range::try_from_byte_range_in_string(text, range, tg::position::Encoding::Utf8)
	else {
		return error;
	};
	let location = tg::error::Location {
		file: tg::error::File::Module(module),
		range,
		symbol: None,
	};
	let mut object = (**error.state().object().unwrap().unwrap_error_ref()).clone();
	object.location = Some(location);
	tg::Error::with_object(object)
}

#[cfg(all(test, feature = "python"))]
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
