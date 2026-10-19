use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub prepare: Option<Prepare>,
}

#[derive(Debug)]
pub struct Prepare {
	pub placeholder: String,
	pub range: tg::Range,
}

impl Compiler {
	pub async fn prepare_rename(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Prepare>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).prepare_rename(self, arg).await?;

		let Some(prepare) = output.prepare else {
			return Ok(None);
		};
		// Reject the same cross-language edits as the rename request.
		if self
			.rename(module, position, prepare.placeholder.clone())
			.await?
			.is_none()
		{
			return Ok(None);
		}

		Ok(Some(prepare))
	}
}

impl Compiler {
	pub(crate) async fn handle_prepare_rename_request(
		&self,
		params: lsp::TextDocumentPositionParams,
	) -> tg::Result<Option<lsp::PrepareRenameResponse>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the position for the request.
		let position = params.position;

		// Get the prepare rename response.
		let prepare = self.prepare_rename(&module, position.into()).await?;
		let Some(prepare) = prepare else {
			return Ok(None);
		};

		let response = lsp::PrepareRenameResponse::RangeWithPlaceholder {
			range: prepare.range.into(),
			placeholder: prepare.placeholder,
		};

		Ok(Some(response))
	}
}
