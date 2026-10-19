use {
	super::{Project, position},
	crate::implementation,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn implementation(
		&self,
		request: &implementation::Request,
	) -> tg::Result<implementation::Response> {
		self.with_language_service(&request.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let params = native::ImplementationParams {
				position: position(request.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_implementations(context, &params, None)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs implementations"))?;
			let locations = self.definition_locations(output)?;
			let response = implementation::Response { locations };

			Ok(response)
		})
	}
}
