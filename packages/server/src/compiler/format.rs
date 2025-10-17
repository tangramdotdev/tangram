use {super::Compiler, lsp_types as lsp, tangram_client as tg};

impl Compiler {
	pub fn format(text: &str) -> tg::Result<String> {
		let allocator = oxc::allocator::Allocator::default();

		// Create the formatter.
		let options = oxc_formatter::FormatOptions::default();
		let formatter = oxc_formatter::Formatter::new(&allocator, options);

		// Parse.
		let source_type = oxc::span::SourceType::ts();
		let output = oxc::parser::Parser::new(&allocator, text, source_type).parse();
		if !output.errors.is_empty() {
			let error = tg::error!("failed to parse the text");
			return Err(error);
		}

		// Format.
		let code = formatter.build(&output.program);

		Ok(code)
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
		let range = tg::Range::try_from_byte_range_in_string(&text, 0..text.len())
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
