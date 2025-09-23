use {
	crate::Server,
	swc_core::{self as swc, ecma::ast::Pass as _},
	tangram_client as tg,
};

#[derive(Debug)]
pub struct Output {
	pub transpiled_text: String,
	pub source_map: String,
}

pub struct Error {
	message: String,
	line: usize,
	column: usize,
}

impl Server {
	pub fn transpile_module(text: String, module: &tg::module::Data) -> tg::Result<Output> {
		let globals = swc::common::Globals::default();
		swc::common::GLOBALS.set(&globals, move || {
			// Parse the text.
			let super::parse::Output {
				mut program,
				source_map,
			} = Self::parse_module(module, text)?;

			let unresolved_mark = swc::common::Mark::new();
			let top_level_mark = swc::common::Mark::new();

			// Create the resolver.
			let resolver = swc::ecma::visit::visit_mut_pass(swc::ecma::transforms::base::resolver(
				unresolved_mark,
				top_level_mark,
				true,
			));

			// Create the stripper.
			let stripper =
				swc::ecma::transforms::typescript::strip(unresolved_mark, top_level_mark);

			// Create the fixer.
			let fixer =
				swc::ecma::visit::visit_mut_pass(swc::ecma::transforms::base::fixer::fixer(None));

			// Visit the module.
			(resolver, stripper, fixer).process(&mut program);

			// Create the writer.
			let mut transpiled_text = Vec::new();
			let mut source_mappings = Vec::new();
			let mut writer = swc::ecma::codegen::text_writer::JsWriter::new(
				source_map.clone(),
				"\n",
				&mut transpiled_text,
				Some(&mut source_mappings),
			);
			writer.set_indent_str("\t");

			// Create the config.
			let config = swc::ecma::codegen::Config::default();

			// Create the emitter.
			let mut emitter = swc::ecma::codegen::Emitter {
				cfg: config,
				comments: None,
				cm: source_map.clone(),
				wr: writer,
			};

			// Emit the module.
			emitter
				.emit_program(&program)
				.map_err(|source| tg::error!(!source, "failed to emit the program"))?;
			let transpiled_text = String::from_utf8(transpiled_text)
				.map_err(|source| tg::error!(!source, "failed to convert bytes to string"))?;

			// Create the source map.
			let mut output_source_map = Vec::new();
			source_map
				.build_source_map(
					&source_mappings,
					None,
					swc::common::source_map::DefaultSourceMapGenConfig,
				)
				.to_writer(&mut output_source_map)
				.map_err(|source| tg::error!(!source, "failed to create the source map"))?;
			let source_map = String::from_utf8(output_source_map)
				.map_err(|source| tg::error!(!source, "failed to convert bytes to string"))?;

			// Create the output.
			let output = Output {
				transpiled_text,
				source_map,
			};

			Ok(output)
		})
	}
}

impl Error {
	#[allow(dead_code)]
	pub fn new(message: impl std::fmt::Display, loc: &swc::common::Loc) -> Self {
		let line = loc.line - 1;
		let column = loc.col_display;
		Self {
			message: message.to_string(),
			line,
			column,
		}
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let line = self.line + 1;
		let column = self.column + 1;
		let message = &self.message;
		write!(f, "{line}:{column} {message}").unwrap();
		Ok(())
	}
}
