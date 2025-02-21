use super::Compiler;
use futures::{TryStreamExt as _, stream::FuturesOrdered};
use lsp_types as lsp;
use tangram_client as tg;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
	pub module: tg::Module,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
	pub locations: Option<Vec<tg::Location>>,
}

impl Compiler {
	pub async fn definition(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::Definition(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Definition(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.locations)
	}

	pub async fn type_definition(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::TypeDefinition(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::TypeDefinition(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.locations)
	}
}

impl Compiler {
	pub(crate) async fn handle_definition_request(
		&self,
		params: lsp::GotoDefinitionParams,
	) -> tg::Result<Option<lsp::GotoDefinitionResponse>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the locations.
		let locations = self.definition(&module, position.into()).await?;

		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the locations.
		let locations = locations
			.into_iter()
			.map(|location| {
				let compiler = self.clone();
				async move {
					Ok::<_, tg::Error>(lsp::Location {
						uri: compiler.lsp_uri_for_module(&location.module).await?,
						range: location.range.into(),
					})
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		let response = lsp::GotoDefinitionResponse::Array(locations);

		Ok(Some(response))
	}

	pub(crate) async fn handle_type_definition_request(
		&self,
		params: lsp::GotoDefinitionParams,
	) -> tg::Result<Option<lsp::GotoDefinitionResponse>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the locations.
		let locations = self.type_definition(&module, position.into()).await?;

		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the locations.
		let locations = locations
			.into_iter()
			.map(|location| {
				let compiler = self.clone();
				async move {
					Ok::<_, tg::Error>(lsp::Location {
						uri: compiler.lsp_uri_for_module(&location.module).await?,
						range: location.range.into(),
					})
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		let response = lsp::GotoDefinitionResponse::Array(locations);

		Ok(Some(response))
	}
}
