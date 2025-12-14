use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

impl Compiler {
	pub fn format(text: &str) -> tg::Result<String> {
		let allocator = oxc::allocator::Allocator::default();
		let source_type = oxc::span::SourceType::ts();
		let options = oxc::parser::ParseOptions {
			preserve_parens: false,
			..Default::default()
		};
		let output = oxc::parser::Parser::new(&allocator, text, source_type)
			.with_options(options)
			.parse();
		let options = oxc_formatter::FormatOptions {
			indent_style: oxc_formatter::IndentStyle::Tab,
			line_width: oxc_formatter::LineWidth::try_from(80).unwrap(),
			..Default::default()
		};
		let formatter = oxc_formatter::Formatter::new(&allocator, options);
		let formatted = formatter.build(&output.program);
		Ok(formatted)
	}
}

impl Compiler {
	pub(super) async fn handle_format_request(
		&self,
		params: lsp::DocumentFormattingParams,
	) -> tg::Result<Option<Vec<lsp::TextEdit>>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Load the module.
		let text = self.load_module(&module).await?;

		// Get the text range.
		let encoding = *self.position_encoding.read().unwrap();
		let range = tg::Range::try_from_byte_range_in_string(&text, 0..text.len(), encoding)
			.ok_or_else(|| tg::error!("failed to create range"))?;

		// Format the text.
		let formatted_text = Self::format(&text)?;

		// Create the edit.
		let edit = lsp::TextEdit {
			range: range.into(),
			new_text: formatted_text,
		};

		Ok(Some(vec![edit]))
	}
}
