use {
	super::{Project, position},
	crate::definition,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn declaration(
		&self,
		request: &definition::Request,
	) -> tg::Result<definition::Response> {
		self.definition(request)
	}

	pub(super) fn definition(
		&self,
		request: &definition::Request,
	) -> tg::Result<definition::Response> {
		self.with_language_service(&request.module, |context, service| {
			let uri = self.uri(&request.module);
			let position = position(request.position);
			let output = service
				.provide_definition(context, &uri, position)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs definitions"))?;
			let locations = self.definition_locations(output)?;
			let response = definition::Response { locations };

			Ok(response)
		})
	}

	pub(super) fn type_definition(
		&self,
		request: &definition::Request,
	) -> tg::Result<definition::Response> {
		self.with_language_service(&request.module, |context, service| {
			let uri = self.uri(&request.module);
			let position = position(request.position);
			let output = service
				.provide_type_definition(context, &uri, position)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs definitions"))?;
			let locations = self.definition_locations(output)?;
			let response = definition::Response { locations };

			Ok(response)
		})
	}

	pub(super) fn definition_locations(
		&self,
		output: native::LocationOrLocationsOrDefinitionLinksOrNull,
	) -> tg::Result<Option<Vec<tg::module::data::Location>>> {
		let locations = output
			.locations
			.or_else(|| output.location.map(|location| vec![location]))
			.or_else(|| {
				output.definition_links.map(|links| {
					links
						.into_iter()
						.map(|link| native::Location {
							range: link.target_selection_range,
							uri: link.target_uri,
						})
						.collect()
				})
			});
		let locations = locations
			.map(|locations| {
				locations
					.into_iter()
					.map(|location| self.location(&location))
					.collect()
			})
			.transpose()?;

		Ok(locations)
	}
}
