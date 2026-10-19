use {super::Project, crate::diagnostics, tangram_client::prelude::*, tsrs_lsproto as native};

impl Project {
	pub(super) fn diagnostics(
		&self,
		arg: diagnostics::DocumentArg,
	) -> tg::Result<diagnostics::DocumentOutput> {
		let mut diagnostics = Vec::new();
		for module in arg.modules {
			let items = self.with_language_service(&module, |context, service| {
				let uri = self.uri(&module);
				let output = service
					.provide_diagnostics(context, &uri)
					.map_err(|error| tg::error!(!error, "failed to get the tsrs diagnostics"))?;
				let items = output
					.full_document_diagnostic_report
					.ok_or_else(|| tg::error!("missing full report in the tsrs diagnostics"))?
					.items
					.into_iter()
					.map(|diagnostic| {
						let location = native::Location {
							range: diagnostic.range,
							uri: uri.clone(),
						};
						let location = Some(self.location(&location)?);
						let message = diagnostic
							.message
							.string
							.or_else(|| {
								diagnostic
									.message
									.markup_content
									.map(|content| content.value)
							})
							.ok_or_else(|| tg::error!("missing message in a tsrs diagnostic"))?;
						let code = diagnostic.code.as_ref().and_then(|code| code.integer);
						let message = normalize_message(code, message);
						let severity = match diagnostic.severity {
							Some(native::DiagnosticSeverity::Error) | None => {
								tg::diagnostic::Severity::Error
							},
							Some(native::DiagnosticSeverity::Hint) => {
								tg::diagnostic::Severity::Hint
							},
							Some(native::DiagnosticSeverity::Information) => {
								tg::diagnostic::Severity::Info
							},
							Some(native::DiagnosticSeverity::Warning) => {
								tg::diagnostic::Severity::Warning
							},
							Some(_) => {
								return Err(tg::error!("invalid severity in a tsrs diagnostic"));
							},
						};
						let diagnostic = tg::diagnostic::Data {
							location,
							message,
							severity,
						};

						Ok(diagnostic)
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Ok(items)
			})?;
			diagnostics.extend(items);
		}
		let output = diagnostics::DocumentOutput { diagnostics };

		Ok(output)
	}

	pub(super) fn diagnostic(
		&self,
		diagnostic: tsrs_core::P<tsrs_ast::Diagnostic>,
	) -> tg::Result<tg::diagnostic::Data> {
		let location = if let (Some(file), Some(start), Some(end)) =
			(diagnostic.file(), diagnostic.pos(), diagnostic.end())
		{
			let module = self
				.host
				.try_module(file.file_name())?
				.ok_or_else(|| tg::error!("unknown module in a tsrs diagnostic"))?
				.without_token();
			let range = tg::Range::try_from_byte_range_in_string(
				file.text(),
				start as usize..end as usize,
				*self.host.compiler.position_encoding.read().unwrap(),
			)
			.ok_or_else(|| tg::error!("invalid range in a tsrs diagnostic"))?;
			Some(tg::module::data::Location { module, range })
		} else {
			None
		};
		let message = tsrs_compiler::diagnosticwriter::flatten_diagnostic_message(diagnostic, "\n");
		let message = normalize_message(Some(diagnostic.code()), message);
		let severity = match diagnostic.category() {
			tsrs_diagnostics::Category::Error => tg::diagnostic::Severity::Error,
			tsrs_diagnostics::Category::Message => tg::diagnostic::Severity::Info,
			tsrs_diagnostics::Category::Suggestion => tg::diagnostic::Severity::Hint,
			tsrs_diagnostics::Category::Warning => tg::diagnostic::Severity::Warning,
		};
		let diagnostic = tg::diagnostic::Data {
			location,
			message,
			severity,
		};

		Ok(diagnostic)
	}
}

fn normalize_message(code: Option<i32>, message: String) -> String {
	match code {
		Some(2732 | 2792) => "cannot find the module".into(),
		_ => message,
	}
}
