use {
	super::{Project, position},
	crate::inlay_hint,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn inlay_hint(
		&self,
		request: &inlay_hint::Request,
	) -> tg::Result<inlay_hint::Response> {
		self.with_language_service(&request.module, |context, service| {
			let range = native::Range {
				end: position(request.range.end),
				start: position(request.range.start),
			};
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let params = native::InlayHintParams {
				range,
				text_document,
				..Default::default()
			};
			let output = service
				.provide_inlay_hint(context, &params)
				.map_err(|error| tg::error!(!error, "failed to get tsrs inlay hints"))?;
			let hints = output
				.inlay_hints
				.map(|hints| {
					hints
						.into_iter()
						.map(|hint| {
							let kind = hint
								.kind
								.map(|kind| match kind {
									native::InlayHintKind::Parameter => {
										Ok(inlay_hint::Kind::Parameter)
									},
									native::InlayHintKind::Type => Ok(inlay_hint::Kind::Type),
									_ => Err(tg::error!("unknown tsrs inlay hint kind")),
								})
								.transpose()?;
							let label = hint.label.string.unwrap_or_else(|| {
								hint.label
									.inlay_hint_label_parts
									.unwrap_or_default()
									.into_iter()
									.map(|part| part.value)
									.collect()
							});
							let position = tg::Position {
								character: hint.position.character,
								line: hint.position.line,
							};
							let hint = inlay_hint::Hint {
								kind,
								label,
								padding_left: hint.padding_left,
								padding_right: hint.padding_right,
								position,
							};
							Ok::<_, tg::Error>(hint)
						})
						.collect()
				})
				.transpose()?;
			let response = inlay_hint::Response { hints };

			Ok(response)
		})
	}
}
