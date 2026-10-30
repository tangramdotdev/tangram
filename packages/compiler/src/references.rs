use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub include_declaration: bool,
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub locations: Option<Vec<tg::module::data::Location>>,
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
		let locations = self
			.references(&module, position.into(), params.context.include_declaration)
			.await?;
		let Some(locations) = locations else {
			return Ok(None);
		};

		// Convert the references, dropping the references in bindings that have no place in the home of an interface.
		let locations = self
			.lsp_locations(
				locations.iter().map(tg::module::Location::to_data),
				module.kind,
			)
			.await?;

		Ok(Some(locations))
	}

	pub async fn references(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
		include_declaration: bool,
	) -> tg::Result<Option<Vec<tg::module::Location>>> {
		// Create the arg.
		let arg = Arg {
			include_declaration,
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).references(self, arg).await?;

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
