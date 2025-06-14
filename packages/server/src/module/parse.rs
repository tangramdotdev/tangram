use crate::Server;
use std::collections::BTreeMap;
use std::rc::Rc;
use swc_core as swc;
use swc_core::common::Spanned;
use swc_core::common::source_map::SmallPos;
use tangram_client as tg;

pub struct Output {
	pub program: swc::ecma::ast::Program,
	pub source_map: Rc<swc::common::SourceMap>,
}

impl Server {
	/// Parse a module.
	pub fn parse_module(text: String, file: tg::error::File) -> tg::Result<Output> {
		// Create the parser.
		let syntax = swc::ecma::parser::TsSyntax::default();
		let syntax = swc::ecma::parser::Syntax::Typescript(syntax);
		let source_map = Rc::new(swc::common::SourceMap::default());
		let source_file = source_map.new_source_file(swc::common::FileName::Anon.into(), text);
		let input = swc::ecma::parser::StringInput::from(&*source_file);
		let mut parser = swc::ecma::parser::Parser::new(syntax, input, None);

		// Parse the text.
		let program = parser.parse_program().map_err(|error| {
			let start_line = source_map
				.lookup_line(error.span().lo)
				.map_err(|_| tg::error!("failed to lookup line"))
				.and_then(|line| {
					line.line
						.try_into()
						.map_err(|_| tg::error!("line number too large"))
				})
				.unwrap();
			let start_column = source_map.lookup_char_pos(error.span().lo).col.to_u32();
			let end_line = source_map
				.lookup_line(error.span().hi)
				.map_err(|_| tg::error!("failed to lookup line"))
				.and_then(|line| {
					line.line
						.try_into()
						.map_err(|_| tg::error!("line number too large"))
				})
				.unwrap();
			let end_column = source_map.lookup_char_pos(error.span().hi).col.to_u32();
			let message = Some(error.into_kind().msg().to_string());
			let location = Some(tg::error::Location {
				symbol: None,
				file,
				range: tg::Range {
					start: tg::Position {
						line: start_line,
						character: start_column,
					},
					end: tg::Position {
						line: end_line,
						character: end_column,
					},
				},
			});
			let stack = None;
			let source = None;
			let values = BTreeMap::new();
			tg::Error {
				code: None,
				message,
				location,
				stack,
				source,
				values,
			}
		})?;

		Ok(Output {
			program,
			source_map,
		})
	}
}
