use {
	super::{Project, position},
	crate::rename,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn rename(&self, request: rename::Request) -> tg::Result<rename::Response> {
		self.with_language_service(&request.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&request.module),
			};
			let params = native::RenameParams {
				new_name: request.new_name,
				position: position(request.position),
				text_document,
				..Default::default()
			};
			let output = service
				.provide_rename(context, &params, None)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs rename locations"))?;
			let edits = output
				.workspace_edit
				.map(|edit| {
					let mut edits = edit
						.changes
						.unwrap_or_default()
						.into_iter()
						.flat_map(|(uri, edits)| {
							edits.into_iter().map(move |edit| (uri.clone(), edit))
						})
						.collect::<Vec<_>>();
					for change in edit.document_changes.unwrap_or_default() {
						let change = change.text_document_edit.ok_or_else(|| {
							tg::error!("unsupported resource change in the tsrs rename")
						})?;
						let uri = change.text_document.uri;
						for edit in change.edits {
							let edit = if let Some(edit) = edit.text_edit {
								edit
							} else if let Some(edit) = edit.annotated_text_edit {
								native::TextEdit {
									new_text: edit.new_text,
									range: edit.range,
								}
							} else {
								return Err(tg::error!(
									"unsupported snippet edit in the tsrs rename"
								));
							};
							edits.push((uri.clone(), edit));
						}
					}
					edits
						.into_iter()
						.map(|(uri, edit)| {
							let location = native::Location {
								range: edit.range,
								uri,
							};
							let location = self.location(&location)?;
							let new_text = edit.new_text;
							let edit = rename::Data { location, new_text };
							Ok(edit)
						})
						.collect::<tg::Result<_>>()
				})
				.transpose()?;
			let response = rename::Response { edits };

			Ok(response)
		})
	}
}
