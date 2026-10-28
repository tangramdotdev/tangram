use {
	super::{Project, position},
	crate::references,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn references(&self, arg: &references::Arg) -> tg::Result<references::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let context_arg = native::ReferenceContext {
				include_declaration: arg.include_declaration,
			};
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&arg.module),
			};
			let params = native::ReferenceParams {
				context: context_arg,
				position: position(arg.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_references(context, &params, None)
				.map_err(|error| tg::error!(!error, "failed to get the javascript references"))?;
			let locations = output
				.locations
				.map(|locations| {
					locations
						.into_iter()
						.map(|location| self.location(&location))
						.collect()
				})
				.transpose()?;
			let output = references::Output { locations };

			Ok(output)
		})
	}
}
