use {
	super::Compiler,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Output {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	pub source_map: String,
	pub text: String,
}

impl Compiler {
	#[must_use]
	pub fn transpile(text: &str, module: &tg::module::Data) -> Output {
		// Create an allocator.
		let allocator = oxc::allocator::Allocator::default();

		let mut diagnostics = Vec::new();

		// Parse.
		let source_type = oxc::span::SourceType::ts();
		let oxc::parser::ParserReturn {
			mut program,
			diagnostics: parse_diagnostics,
			..
		} = oxc::parser::Parser::new(&allocator, text, source_type).parse();
		for error in &parse_diagnostics {
			diagnostics.push(crate::util::convert_diagnostic(error, module, text));
		}

		// Get semantic analysis.
		let oxc::semantic::SemanticBuilderReturn {
			semantic,
			diagnostics: semantic_diagnostics,
		} = oxc::semantic::SemanticBuilder::new().build(&program);
		for error in &semantic_diagnostics {
			diagnostics.push(crate::util::convert_diagnostic(error, module, text));
		}

		// Transform.
		let path = Path::new("module.ts");
		let options = oxc::transformer::TransformOptions::default();
		let oxc::transformer::TransformerReturn {
			diagnostics: transform_diagnostics,
			..
		} = oxc::transformer::Transformer::new(&allocator, path, &options)
			.build_with_scoping(semantic.into_scoping(), &mut program);
		for error in &transform_diagnostics {
			diagnostics.push(crate::util::convert_diagnostic(error, module, text));
		}

		// Generate the output code with source maps.
		let name = module.without_token().to_string();
		let options = oxc::codegen::CodegenOptions {
			source_map_path: Some(PathBuf::from(name)),
			..Default::default()
		};
		let output = oxc::codegen::Codegen::new()
			.with_options(options)
			.with_source_text(text)
			.build(&program);
		let text = output.code;

		// Remove source contents so that generated capability-bearing source is not exposed.
		let mut source_map = output.map.unwrap();
		let source_count = source_map.get_sources().len();
		source_map.set_source_contents(vec![None; source_count]);
		let source_map = source_map.to_json_string();

		Output {
			diagnostics,
			source_map,
			text,
		}
	}
}
