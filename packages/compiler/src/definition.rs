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
	pub async fn declaration(
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
		let output = self.backend(module.kind).declaration(self, arg).await?;

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

	pub async fn definition(
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
		let output = self.backend(module.kind).definition(self, arg).await?;

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

	pub async fn type_definition(
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
		let output = self.backend(module.kind).type_definition(self, arg).await?;

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
	pub(crate) async fn handle_declaration_request(
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
		let locations = self.declaration(&module, position.into()).await?;

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
			.try_collect::<Vec<_>>()
			.await?;

		// Remove duplicate locations, such as several declarations in bindings that map to the same source.
		let mut unique = Vec::new();
		for location in locations {
			if !unique.contains(&location) {
				unique.push(location);
			}
		}

		let response = lsp::GotoDefinitionResponse::Array(unique);

		Ok(Some(response))
	}

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
				let kind = module.kind;
				let compiler = self.clone();
				async move {
					compiler
						.lsp_location_with_language(&location.to_data(), kind)
						.await
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Remove duplicate locations, such as several declarations in bindings that map to the same source.
		let mut unique = Vec::new();
		for location in locations {
			if !unique.contains(&location) {
				unique.push(location);
			}
		}

		let response = lsp::GotoDefinitionResponse::Array(unique);

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
				let kind = module.kind;
				let compiler = self.clone();
				async move {
					compiler
						.lsp_location_with_language(&location.to_data(), kind)
						.await
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Remove duplicate locations, such as several declarations in bindings that map to the same source.
		let mut unique = Vec::new();
		for location in locations {
			if !unique.contains(&location) {
				unique.push(location);
			}
		}

		let response = lsp::GotoDefinitionResponse::Array(unique);

		Ok(Some(response))
	}
}
