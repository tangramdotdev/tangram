use {
	super::{Project, position, range},
	crate::selection_range,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn selection_range(
		&self,
		request: selection_range::Request,
	) -> tg::Result<selection_range::Response> {
		self.with_language_service(&request.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let positions = request.positions.into_iter().map(position).collect();
			let params = native::SelectionRangeParams {
				positions,
				text_document,
				..Default::default()
			};
			let output = service
				.provide_selection_ranges(context, &params)
				.map_err(|error| tg::error!(!error, "failed to get tsrs selection ranges"))?;
			let ranges = output
				.selection_ranges
				.map(|ranges| ranges.into_iter().map(selection).collect());
			let response = selection_range::Response { ranges };

			Ok(response)
		})
	}
}

fn selection(selection: native::SelectionRange) -> selection_range::SelectionRange {
	let parent = selection
		.parent
		.map(|parent| Box::new(self::selection(*parent)));
	let range = range(selection.range);

	selection_range::SelectionRange { parent, range }
}
