use {
	super::{Project, position},
	crate::definition,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn declaration(&self, arg: &definition::Arg) -> tg::Result<definition::Output> {
		self.definition(arg)
	}

	pub(super) fn definition(&self, arg: &definition::Arg) -> tg::Result<definition::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let uri = self.uri(&arg.module);
			let position = position(arg.position);
			let output = service
				.provide_definition(context, &uri, position)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs definitions"))?;
			let locations = self.definition_locations(output)?;
			let output = definition::Output { locations };

			Ok(output)
		})
	}

	pub(super) fn type_definition(&self, arg: &definition::Arg) -> tg::Result<definition::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let uri = self.uri(&arg.module);
			let position = position(arg.position);
			let output = service
				.provide_type_definition(context, &uri, position)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs definitions"))?;
			let locations = self.definition_locations(output)?;
			let output = definition::Output { locations };

			Ok(output)
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
