use {
	super::{Project, position},
	crate::implementation,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn implementation(
		&self,
		arg: &implementation::Arg,
	) -> tg::Result<implementation::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&arg.module),
			};
			let params = native::ImplementationParams {
				position: position(arg.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_implementations(context, &params, None)
				.map_err(|error| {
					tg::error!(!error, "failed to get the tsrs implementations")
				})?;
			let locations = self.definition_locations(output)?;
			let output = implementation::Output { locations };

			Ok(output)
		})
	}
}
