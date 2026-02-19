use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub prepare: Option<Prepare>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Prepare {
	pub range: tg::Range,
	pub placeholder: String,
}

impl Compiler {
	pub async fn prepare_rename(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Prepare>> {
		// Create the request.
		let request = super::Request::PrepareRename(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::PrepareRename(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.prepare)
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
