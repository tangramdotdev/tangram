use {
	super::Database,
	ruff_db::{diagnostic::UnifiedFile, files::File, source::source_text},
	tangram_client::prelude::*,
	ty_python_semantic::Db as _,
	ty_text_size::Ranged as _,
};

impl Database {
	pub(super) fn document_diagnostics(
		&self,
		modules: Vec<tg::module::Data>,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::diagnostics::DocumentResponse> {
		let mut diagnostics = Vec::new();
		for module in modules {
			let file = self.query_file(&module)?;
			if let Some(entry) = self.entry(file) {
				for diagnostic in &entry.diagnostics {
					let mut diagnostic = diagnostic.to_data();
					if let Some(location) = &mut diagnostic.location {
						let text = source_text(self, file);
						let bytes = location
							.range
							.try_to_byte_range_in_string(&text, tg::position::Encoding::Utf8)
							.ok_or_else(|| tg::error!("invalid Python diagnostic range"))?;
						location.range =
							tg::Range::try_from_byte_range_in_string(&text, bytes, encoding)
								.ok_or_else(|| tg::error!("invalid Python diagnostic range"))?;
					}
					diagnostics.push(diagnostic);
				}
			}
			let results = ty_python_semantic::check_file(self, self.program_file(file))
				.unwrap_or_else(|error| vec![error].into_boxed_slice());
			for diagnostic in &results {
				diagnostics.push(self.diagnostic(diagnostic, encoding)?.to_data());
			}
		}
		if let Some(error) = self.error.lock().unwrap().take() {
			return Err(error);
		}
		Ok(crate::diagnostics::DocumentResponse { diagnostics })
	}

	pub(super) fn code_actions(
		&self,
		file: File,
		request: &crate::code_action::Request,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::code_action::Response> {
		if request.only.as_ref().is_some_and(|kinds| {
			!kinds
				.iter()
				.any(|kind| kind.is_empty() || kind == "quickfix")
		}) {
			return Ok(crate::code_action::Response { actions: None });
		}
		let start = self.offset(file, request.range.start, encoding)?;
		let end = self.offset(file, request.range.end, encoding)?;
		let program_file = self.program_file(file);
		let diagnostics = ty_python_semantic::check_file(self, program_file)
			.unwrap_or_else(|error| vec![error].into_boxed_slice());
		let mut actions = Vec::new();
		for diagnostic in &diagnostics {
			let Some(span) = diagnostic.primary_span() else {
				continue;
			};
			let Some(range) = span.range() else {
				continue;
			};
			if span.file() != &UnifiedFile::Ty(file) || range.end() < start || range.start() > end {
				continue;
			}
			let Ok(lint) = self.lint_registry().get(diagnostic.id().as_str()) else {
				continue;
			};
			let title = format!("Ignore '{}' for this line", lint.name());
			let edits = ty_python_semantic::suppress_single(
				self,
				program_file.python_file(self),
				lint,
				range,
			)
			.into_edits()
			.into_iter()
			.map(|edit| {
				let range = self.range(file, edit.range(), encoding)?;
				let new_text = edit.content().unwrap_or_default().to_owned();
				let module = request.module.clone();
				Ok(crate::code_action::Edit {
					module,
					new_text,
					range,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
			let action = crate::code_action::Action {
				edits: Some(edits),
				kind: Some("quickfix".to_owned()),
				title,
			};
			actions.push(action);
		}
		Ok(crate::code_action::Response {
			actions: Some(actions),
		})
	}
}
