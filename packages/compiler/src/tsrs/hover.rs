use {
	super::{Project, position},
	crate::hover,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn hover(&self, request: &hover::Request) -> tg::Result<hover::Response> {
		self.with_language_service(&request.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let params = native::HoverParams {
				position: position(request.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_hover(context, &params)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs hover"))?;
			let text = output.hover.and_then(|hover| {
				let contents = hover.contents;
				contents
					.marked_string_with_language
					.map(|value| value.value)
					.or(contents.string)
					.or_else(|| contents.markup_content.map(|value| value.value))
					.or_else(|| {
						contents.marked_strings.map(|values| {
							values
								.into_iter()
								.filter_map(|value| {
									value.string.or_else(|| {
										value.marked_string_with_language.map(|value| value.value)
									})
								})
								.collect::<Vec<_>>()
								.join("\n")
						})
					})
			});
			let response = hover::Response { text };

			Ok(response)
		})
	}
}
