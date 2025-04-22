use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

impl Compiler {
	pub async fn format(text: String) -> tg::Result<String> {
		let source_type = biome_js_syntax::JsFileSource::ts();
		let options = biome_js_parser::JsParserOptions::default();
		let node = biome_js_parser::parse(&text, source_type, options);
		let options = biome_js_formatter::context::JsFormatOptions::new(source_type);
		let formatted = biome_js_formatter::format_node(options, &node.syntax())
			.map_err(|source| tg::error!(!source, "failed to format"))?;
		let text = formatted
			.print()
			.map_err(|source| tg::error!(!source, "failed to format"))?
			.into_code();
		Ok(text)
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
		let range = tg::Range::from_byte_range_in_string(&text, 0..text.len());

		// Format the text.
		let formatted_text = Self::format(text).await?;

		// Create the edit.
		let edit = lsp::TextEdit {
			range: range.into(),
			new_text: formatted_text,
		};

		Ok(Some(vec![edit]))
	}
}
