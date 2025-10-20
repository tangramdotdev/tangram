use tangram_client as tg;

pub struct Output {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	pub text: String,
}

#[must_use]
pub fn format(module: &tg::module::Data, text: &str) -> Output {
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
