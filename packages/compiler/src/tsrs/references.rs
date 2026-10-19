use {
	super::{Project, position},
	crate::references,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn references(
		&self,
		request: &references::Request,
	) -> tg::Result<references::Response> {
		self.with_language_service(&request.module, |context, service| {
			let context_arg = native::ReferenceContext {
				include_declaration: request.include_declaration,
			};
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let params = native::ReferenceParams {
				context: context_arg,
				position: position(request.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_references(context, &params, None)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs references"))?;
			let locations = output
				.locations
				.map(|locations| {
					locations
						.into_iter()
						.map(|location| self.location(&location))
						.collect()
				})
				.transpose()?;
			let response = references::Response { locations };

			Ok(response)
		})
	}
}
