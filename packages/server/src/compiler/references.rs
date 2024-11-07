use super::Compiler;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
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
	pub(super) async fn handle_references_request(
		&self,
		params: lsp::ReferenceParams,
	) -> tg::Result<Option<Vec<lsp::Location>>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position.position;

		// Get the references.
		let locations = self.references(&module, position.into()).await?;
		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the reference.
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

		Ok(Some(locations))
	}

	pub async fn references(
		&self,
		module: &tg::Module,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::References(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::References(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.locations)
	}
}
