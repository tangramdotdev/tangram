use {super::Compiler, lsp_types as lsp, tangram_client as tg};

impl Compiler {
	pub fn format(text: &str) -> tg::Result<String> {
		let module = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent::with_item(tg::module::data::Item::Path("module.ts".into())),
		};
		let output = tangram_module::format(&module, text);
		if !output.diagnostics.is_empty() {
			let diagnostics = output
				.diagnostics
				.into_iter()
				.filter_map(|diagnostic| diagnostic.try_into().ok())
				.collect();
			let error = tg::error!(diagnostics = diagnostics, "failed to format the module");
			return Err(error);
		}
		Ok(output.text)
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
