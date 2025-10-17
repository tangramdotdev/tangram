use {crate::Server, tangram_client as tg};

impl Server {
	#[must_use]
	pub fn convert_diagnostic(
		diagnostic: &oxc::diagnostics::OxcDiagnostic,
		module: &tg::module::Data,
		text: &str,
	) -> tg::diagnostic::Data {
		let span = diagnostic
			.labels
			.as_ref()
			.and_then(|labels| labels.first())
			.map_or(oxc::span::Span::new(0, 0), |label| {
				oxc::span::Span::new(
					label.offset().try_into().unwrap(),
					(label.offset() + label.len()).try_into().unwrap(),
				)
			});
		let byte_range = span.start as usize..span.end as usize;
		let range =
			tg::Range::try_from_byte_range_in_string(text, byte_range).unwrap_or(tg::Range {
				start: tg::Position {
					line: 0,
					character: 0,
				},
				end: tg::Position {
					line: 0,
					character: 0,
				},
			});
		let location = Some(tg::location::Data {
			module: module.clone(),
			range,
		});
		let message = diagnostic.message.as_ref().to_owned();
		let severity = match diagnostic.severity {
			oxc::diagnostics::Severity::Advice => tg::diagnostic::Severity::Hint,
			oxc::diagnostics::Severity::Warning => tg::diagnostic::Severity::Warning,
			oxc::diagnostics::Severity::Error => tg::diagnostic::Severity::Error,
		};
		tg::diagnostic::Data {
			location,
			message,
			severity,
		}
	}
}
