use {
	super::Compiler,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub locations: Option<Vec<tg::module::data::Location>>,
}

impl Compiler {
	pub async fn implementation(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<tg::module::Location>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).implementation(self, arg).await?;

		// Convert locations from data to the non-serializable form.
		let locations = output
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
				let kind = module.kind;
				let compiler = self.clone();
				async move {
					compiler
						.lsp_location_with_language(&location.to_data(), kind)
						.await
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		let response = lsp::GotoDefinitionResponse::Array(locations);

		Ok(Some(response))
	}
}
