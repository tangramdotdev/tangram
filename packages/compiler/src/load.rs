#[cfg(not(feature = "py"))]
use tangram_client::prelude::*;
#[cfg(feature = "py")]
use {std::fmt::Write as _, tangram_client::prelude::*};

#[cfg(feature = "py")]
mod js;
#[cfg(feature = "py")]
mod py;

/// Generate the consuming language's representation without changing the module's identity.
#[cfg(feature = "py")]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	let exports = match (language, module.kind) {
		(Some(tg::module::load::Language::Js), tg::module::Kind::Py) => py::exports(module, text),
		(Some(tg::module::load::Language::Py), tg::module::Kind::Js | tg::module::Kind::Ts) => {
			js::exports(module, text)
		},
		(Some(tg::module::load::Language::Py), tg::module::Kind::Dts) => {
			return Err(tg::error!("cannot execute a declaration module in Python"));
		},
		_ => return Ok(text.to_owned()),
	};
	let exports = exports.map_err(|error| tg::error!(!error, module = ?module.without_token(), "failed to discover the cross-language exports"))?;

	// Keep capabilities in the executable wrapper, just as for synthetic object modules.
	let data = serde_json::to_string(module).unwrap();
	let mut output = String::new();
	match language.unwrap() {
		tg::module::load::Language::Js => {
			for (index, name) in exports.iter().enumerate() {
				let name = serde_json::to_string(name).unwrap();
				writeln!(output, "const f{index} = tg.Command.function(tg.Module.fromData({data}), {name});\nexport {{ f{index} as {name} }};").unwrap();
			}
			output.push_str("export {};\n");
		},
		tg::module::load::Language::Py => {
			// Choose helper names that cannot shadow an export.
			let mut prefix = "_tg".to_owned();
			while exports.iter().any(|name| name.starts_with(&prefix)) {
				prefix.push('_');
			}
			writeln!(
				output,
				"import json as {prefix}_json\nimport tangram as {prefix}_client"
			)
			.unwrap();
			let data = serde_json::to_string(&data).unwrap();
			writeln!(
				output,
				"{prefix}_module = {prefix}_client.Module.from_data({prefix}_json.loads({data}))"
			)
			.unwrap();
			for name in &exports {
				if name == "__all__" {
					return Err(tg::error!(
						"the export name __all__ is reserved by the Python loader"
					));
				}
				// Python cannot bind a JavaScript export whose name is not a Python identifier.
				let binding = format!("{name} = None");
				let parsed = ruff_python_parser::parse_module(&binding).map_err(|error| {
					tg::error!(
						!error,
						export = %name,
						"the export name is not a Python identifier"
					)
				})?;
				if !matches!(parsed.syntax().body.as_slice(), [ruff_python_ast::Stmt::Assign(assign)] if matches!(assign.targets.as_slice(), [ruff_python_ast::Expr::Name(target)] if target.id.as_str() == name))
				{
					return Err(tg::error!(
						export = %name,
						"the export name is not a Python identifier"
					));
				}
				let quoted = serde_json::to_string(name).unwrap();
				writeln!(
					output,
					"{name} = {prefix}_client.Command.function({prefix}_module, {quoted})"
				)
				.unwrap();
			}
			let names = serde_json::to_string(&exports).unwrap();
			writeln!(output, "__all__ = {names}").unwrap();
		},
	}
	Ok(output)
}

#[cfg(not(feature = "py"))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	if module.kind == tg::module::Kind::Py || language == Some(tg::module::load::Language::Py) {
		return Err(tg::error!("the py feature is not enabled"));
	}
	Ok(text.to_owned())
}

#[cfg(feature = "py")]
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

#[cfg(all(test, feature = "py"))]
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
		let source = source(tg::module::Kind::Py);
		let text = "def default(): return 42\n";
		assert_eq!(module(&source, text, None).unwrap(), text);
		assert_eq!(
			module(&source, text, Some(tg::module::load::Language::Py)).unwrap(),
			text
		);
	}

	#[test]
	fn generates_parseable_modules_in_both_languages() {
		let source = source(tg::module::Kind::Py);
		let text = module(
			&source,
			"def default(): return 42\ndef f0(): pass\n",
			Some(tg::module::load::Language::Js),
		)
		.unwrap();
		let result = crate::Compiler::transpile(&text, &source);
		assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
		let mut source = source;
		source.kind = tg::module::Kind::Ts;
		let text = module(
			&source,
			"export default () => 42; export const _tg_client = () => 1;",
			Some(tg::module::load::Language::Py),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(!text.contains("import tangram as _tg_client\n"));
		assert!(text.contains("_tg_client = "));
		assert!(text.contains("default = "));
	}

	#[test]
	fn rejects_unrepresentable_python_exports() {
		let source = source(tg::module::Kind::Ts);
		for text in [
			"const f = () => 42; export { f as 'not-an-identifier' };",
			"export const __all__ = () => 42;",
			"export const lambda = () => 42;",
		] {
			assert!(module(&source, text, Some(tg::module::load::Language::Py)).is_err());
		}
	}
}
