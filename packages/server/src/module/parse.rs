use crate::Server;
use std::rc::Rc;
use swc_core as swc;
use tangram_client as tg;

pub struct Output {
	pub program: swc::ecma::ast::Program,
	pub source_map: Rc<swc::common::SourceMap>,
}

impl Server {
	/// Parse a module.
	pub fn parse_module(text: String) -> tg::Result<Output> {
		// Create the parser.
		let syntax = swc::ecma::parser::TsSyntax::default();
		let syntax = swc::ecma::parser::Syntax::Typescript(syntax);
		let source_map = Rc::new(swc::common::SourceMap::default());
		let source_file = source_map.new_source_file(swc::common::FileName::Anon.into(), text);
		let input = swc::ecma::parser::StringInput::from(&*source_file);
		let mut parser = swc::ecma::parser::Parser::new(syntax, input, None);

		// Parse the text.
		let program = parser
			.parse_program()
			.map_err(|error| tg::error!("{}", error.into_kind().msg()))?;

		Ok(Output {
			program,
			source_map,
		})
	}
}
