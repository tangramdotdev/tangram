use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

impl Compiler {
	#[cfg(not(feature = "biome"))]
	pub fn format(_text: &str) -> tg::Result<String> {
		Err(tg::error!("biome is not enabled"))
	}

	#[cfg(feature = "biome")]
	pub fn format(text: &str) -> tg::Result<String> {
		let source_type = biome_js_syntax::JsFileSource::ts();
		let options = biome_js_parser::JsParserOptions::default();
		let node = biome_js_parser::parse(text, source_type, options);
		let options = biome_js_formatter::context::JsFormatOptions::new(source_type);
		let formatted = biome_js_formatter::format_node(options, &node.syntax())
			.map_err(|source| tg::error!(!source, "failed to format the module"))?;
		let text = formatted
			.print()
			.map_err(|source| tg::error!(!source, "failed to format the module"))?;
		Ok(text.into_code())
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
