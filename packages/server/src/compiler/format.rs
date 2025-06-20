use super::Compiler;
use lsp_types as lsp;
use tangram_client as tg;

impl Compiler {
	#[cfg(feature = "format")]
	pub fn format(text: &str) -> tg::Result<String> {
		#[cfg(feature = "js-formatting")]
		{
			let source_type = biome_js_syntax::JsFileSource::ts();
			let options = biome_js_parser::JsParserOptions::default();
			let node = biome_js_parser::parse(text, source_type, options);
			let options = biome_js_formatter::context::JsFormatOptions::new(source_type);
			let formatted = biome_js_formatter::format_node(options, &node.syntax())
				.map_err(|source| tg::error!(!source, "failed to format"))?;
			let text = formatted
				.print()
				.map_err(|source| tg::error!(!source, "failed to format"))?
				.into_code();
			Ok(text)
		}

		#[cfg(not(feature = "js-formatting"))]
		{
			let _ = text;
			Err(tg::error!(
				"JavaScript formatting is not available - compile with the 'js-formatting' feature to enable"
			))
		}
	}

	#[cfg(not(feature = "format"))]
	pub fn format(_text: &str) -> tg::Result<String> {
		Err(tg::error!("formatting not enabled"))
	}
}

impl Compiler {
	pub(super) async fn handle_format_request(
		&self,
		params: lsp::DocumentFormattingParams,
	) -> tg::Result<Option<Vec<lsp::TextEdit>>> {
		#[cfg(feature = "js-formatting")]
		{
			// Get the module.
			let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

			// Load the module.
			let text = self.load_module(&module).await?;

			// Get the text range.
			let range = tg::Range::from_byte_range_in_string(&text, 0..text.len());

			// Format the text.
			let formatted_text = Self::format(&text)?;

			// Create the edit.
			let edit = lsp::TextEdit {
				range: range.into(),
				new_text: formatted_text,
			};

			Ok(Some(vec![edit]))
		}

		#[cfg(not(feature = "js-formatting"))]
		{
			let _ = (self, params);
			Err(tg::error!(
				"JavaScript formatting is not available - compile with the 'js-formatting' feature to enable"
			))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_format_with_feature() {
		#[cfg(feature = "js-formatting")]
		{
			let input = "const x=1;let y=2;";
			let result = Compiler::format(input);
			assert!(result.is_ok());
			let formatted = result.unwrap();
			// The formatted code should be different from the input
			assert_ne!(formatted, input);
			// Should contain proper formatting
			assert!(formatted.contains("const x = 1;"));
		}
	}

	#[test]
	fn test_format_without_feature() {
		#[cfg(not(feature = "js-formatting"))]
		{
			let input = "const x=1;let y=2;";
			let result = Compiler::format(input);
			assert!(result.is_err());
			let error = result.unwrap_err();
			assert!(
				error
					.to_string()
					.contains("JavaScript formatting is not available")
			);
		}
	}
}
