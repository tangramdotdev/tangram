use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Output {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	pub source_map: String,
	pub text: String,
}

#[must_use]
pub fn transpile(text: &str, module: &tg::module::Data) -> Output {
	// Create an allocator.
	let allocator = oxc::allocator::Allocator::default();

	let mut diagnostics = Vec::new();

	// Parse.
	let source_type = oxc::span::SourceType::ts();
	let oxc::parser::ParserReturn {
		mut program,
		errors,
		..
	} = oxc::parser::Parser::new(&allocator, text, source_type).parse();
	for error in &errors {
		diagnostics.push(crate::diagnostic::convert(error, module, text));
	}

	// Get semantic analysis.
	let oxc::semantic::SemanticBuilderReturn { semantic, errors } =
		oxc::semantic::SemanticBuilder::new().build(&program);
	for error in &errors {
		diagnostics.push(crate::diagnostic::convert(error, module, text));
	}

	// Transform.
	let path = Path::new("module.ts");
	let options = oxc::transformer::TransformOptions::default();
	let oxc::transformer::TransformerReturn { errors, .. } =
		oxc::transformer::Transformer::new(&allocator, path, &options)
			.build_with_scoping(semantic.into_scoping(), &mut program);
	for error in &errors {
		diagnostics.push(crate::diagnostic::convert(error, module, text));
	}

	// Generate the output code with source maps.
	let name = match &module.referent.item {
		tg::module::data::Item::Path(path) => path.to_str().unwrap().to_owned(),
		tg::module::data::Item::Object(_) => "module.ts".to_owned(),
	};
	let options = oxc::codegen::CodegenOptions {
		source_map_path: Some(PathBuf::from(format!("{name}.map"))),
		..Default::default()
	};
	let output = oxc::codegen::Codegen::new()
		.with_options(options)
		.with_source_text(text)
		.build(&program);
	let text = output.code;
	let source_map = output.map.unwrap().to_json_string();

	Output {
		diagnostics,
		source_map,
		text,
	}
}
