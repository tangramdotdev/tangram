use tangram_client as tg;

pub struct Output {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	pub text: String,
}

#[must_use]
pub fn format(_module: &tg::module::Data, text: &str) -> Output {
	let source_type = biome_js_syntax::JsFileSource::ts();
	let options = biome_js_parser::JsParserOptions::default();
	let node = biome_js_parser::parse(text, source_type, options);
	let options = biome_js_formatter::context::JsFormatOptions::new(source_type);
	let formatted = match biome_js_formatter::format_node(options, &node.syntax()) {
		Ok(formatted) => formatted,
		Err(error) => {
			let diagnostics = vec![tg::diagnostic::Data {
				location: None,
				severity: tg::diagnostic::Severity::Error,
				message: error.to_string(),
			}];
			return Output {
				diagnostics,
				text: text.to_owned(),
			};
		},
	};
	let text = match formatted.print() {
		Ok(printed) => printed.into_code(),
		Err(error) => {
			let diagnostics = vec![tg::diagnostic::Data {
				location: None,
				severity: tg::diagnostic::Severity::Error,
				message: error.to_string(),
			}];
			return Output {
				diagnostics,
				text: text.to_owned(),
			};
		},
	};
	let diagnostics = Vec::new();
	Output { diagnostics, text }
}

#[must_use]
pub fn format_oxc(module: &tg::module::Data, text: &str) -> Output {
	let allocator = oxc::allocator::Allocator::default();

	// Create the formatter.
	let options = oxc_formatter::FormatOptions::default();
	let formatter = oxc_formatter::Formatter::new(&allocator, options);

	// Parse.
	let source_type = oxc::span::SourceType::ts();
	let output = oxc::parser::Parser::new(&allocator, text, source_type).parse();
	let diagnostics = output
		.errors
		.iter()
		.map(|diagnostic| crate::diagnostic::convert(diagnostic, module, text))
		.collect();

	// Format.
	let text = formatter.build(&output.program);

	Output { diagnostics, text }
}
