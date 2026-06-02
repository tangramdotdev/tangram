use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

impl Compiler {
	pub fn format(text: &str) -> tg::Result<String> {
		let allocator = oxc::allocator::Allocator::default();
		let source_type = oxc::span::SourceType::ts();
		let options = oxc_formatter::JsFormatOptions {
			indent_style: "tab".parse().unwrap(),
			line_width: 80.try_into().unwrap(),
			..Default::default()
		};
		let formatted = oxc_formatter::format(&allocator, text, source_type, options, None)
			.map_err(|error| tg::error!(!error, "failed to format the module"))?
			.print()
			.map_err(|error| tg::error!(source = error, "failed to print the formatted module"))?
			.into_code();
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
