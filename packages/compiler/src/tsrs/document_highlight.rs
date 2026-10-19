use {
	super::{Project, position, range},
	crate::document_highlight,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn document_highlight(
		&self,
		arg: &document_highlight::Arg,
	) -> tg::Result<document_highlight::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let uri = self.uri(&arg.module);
			let output = service
				.provide_document_highlights(context, &uri, position(arg.position))
				.map_err(|error| tg::error!(!error, "failed to get tsrs document highlights"))?;
			let highlights = output
				.document_highlights
				.map(|highlights| {
					highlights
						.into_iter()
						.map(|highlight| {
							let kind = highlight
								.kind
								.map(|kind| match kind {
									native::DocumentHighlightKind::Read => {
										Ok(document_highlight::Kind::Read)
									},
									native::DocumentHighlightKind::Text => {
										Ok(document_highlight::Kind::Text)
									},
									native::DocumentHighlightKind::Write => {
										Ok(document_highlight::Kind::Write)
									},
									_ => Err(tg::error!("unknown tsrs document highlight kind")),
								})
								.transpose()?;
							let range = range(highlight.range);
							let highlight = document_highlight::Highlight { kind, range };
							Ok::<_, tg::Error>(highlight)
						})
						.collect()
				})
				.transpose()?;
			let output = document_highlight::Output { highlights };

			Ok(output)
		})
	}
}
