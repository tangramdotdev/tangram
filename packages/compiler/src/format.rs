use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

impl Compiler {
	pub fn format(text: &str, kind: tg::module::Kind) -> tg::Result<String> {
		match kind {
			tg::module::Kind::Artifact
			| tg::module::Kind::Blob
			| tg::module::Kind::Command
			| tg::module::Kind::Directory
			| tg::module::Kind::Error
			| tg::module::Kind::File
			| tg::module::Kind::Graph
			| tg::module::Kind::Object
			| tg::module::Kind::Symlink => Err(tg::error!(%kind, "cannot format the module")),
			tg::module::Kind::Dts | tg::module::Kind::Js | tg::module::Kind::Ts => {
				Self::format_js(text)
			},
			tg::module::Kind::Py => {
				#[cfg(feature = "py")]
				{
					Self::format_py(text)
				}
				#[cfg(not(feature = "py"))]
				{
					Err(tg::error!("the py feature is not enabled"))
				}
			},
		}
	}

	fn format_js(text: &str) -> tg::Result<String> {
		let allocator = oxc::allocator::Allocator::default();
		let source_type = oxc::span::SourceType::ts();
		let options = oxc_formatter::JsFormatOptions {
			indent_style: oxc_formatter_core::IndentStyle::Tab,
			line_width: 80.try_into().unwrap(),
			..Default::default()
		};
		let formatted = oxc_formatter::format(&allocator, text, source_type, options)
			.map_err(|error| tg::error!(!error, "failed to format the module"))?
			.print()
			.map_err(|error| tg::error!(source = error, "failed to print the formatted module"))?
			.into_code();
		Ok(formatted)
	}

	#[cfg(feature = "py")]
	fn format_py(text: &str) -> tg::Result<String> {
		let options = ruff_python_formatter::PyFormatOptions::default()
			.with_target_version(ruff_python_ast::PythonVersion::PY314);
		let formatted = ruff_python_formatter::format_module_source(text, options)
			.map_err(|error| tg::error!(!error, "failed to format the module"))?
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
		let formatted_text = Self::format(&text, module.kind)?;

		// Create the edit.
		let edit = lsp::TextEdit {
			range: range.into(),
			new_text: formatted_text,
		};

		Ok(Some(vec![edit]))
	}
}
