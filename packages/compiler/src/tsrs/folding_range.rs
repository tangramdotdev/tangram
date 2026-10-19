use {super::Project, crate::folding_range, tangram_client::prelude::*, tsrs_lsproto as native};

impl Project {
	pub(super) fn folding_range(
		&self,
		request: &folding_range::Request,
	) -> tg::Result<folding_range::Response> {
		self.with_language_service(&request.module, |context, service| {
			let uri = self.uri(&request.module);
			let output = service
				.provide_folding_range(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get tsrs folding ranges"))?;
			let ranges = output
				.folding_ranges
				.map(|ranges| {
					ranges
						.into_iter()
						.map(|range| {
							let kind = range
								.kind
								.map(|kind| match kind {
									native::FoldingRangeKind::Comment => {
										Ok(folding_range::Kind::Comment)
									},
									native::FoldingRangeKind::Imports => {
										Ok(folding_range::Kind::Imports)
									},
									native::FoldingRangeKind::Region => {
										Ok(folding_range::Kind::Region)
									},
									_ => Err(tg::error!("unknown tsrs folding range kind")),
								})
								.transpose()?;
							let range = folding_range::Range {
								end_character: range.end_character,
								end_line: range.end_line,
								kind,
								start_character: range.start_character,
								start_line: range.start_line,
							};
							Ok::<_, tg::Error>(range)
						})
						.collect()
				})
				.transpose()?;
			let response = folding_range::Response { ranges };

			Ok(response)
		})
	}
}
