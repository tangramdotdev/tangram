use {
	super::Compiler,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub locations: Option<Vec<tg::location::Data>>,
}

impl Compiler {
	pub async fn implementation(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::Location>>> {
		// Create the request.
		let request = super::Request::Implementation(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Implementation(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		// Convert locations from data to the non-serializable form.
		let locations = response
			.locations
			.map(|locations| {
				locations
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<Vec<_>>>()
			})
			.transpose()?;

		Ok(locations)
	}
}

impl Compiler {
	pub(crate) async fn handle_implementation_request(
		&self,
		params: lsp::request::GotoImplementationParams,
	) -> tg::Result<Option<lsp::request::GotoImplementationResponse>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the locations.
		let locations = self.implementation(&module, position.into()).await?;
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
						uri: compiler
							.lsp_uri_for_module(&location.module.to_data())
							.await?,
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
