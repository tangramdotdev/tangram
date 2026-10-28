use {
	super::{Project, position},
	crate::hover,
	tangram_client::prelude::*,
	tsrs_core::json::Value,
	tsrs_lsproto::{self as native, json::Json as _},
};

impl Project {
	pub(super) fn hover(&self, arg: &hover::Arg) -> tg::Result<hover::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let text_document = native::TextDocumentIdentifier {
				uri: self.uri(&arg.module),
			};
			let params = native::HoverParams {
				position: position(arg.position),
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
					.and_then(|value| marked_string_text(value.to_json()))
					.or(contents.string)
					.or_else(|| contents.markup_content.map(|value| value.value))
					.or_else(|| {
						contents.marked_strings.map(|values| {
							values
								.into_iter()
								.filter_map(|value| marked_string_text(value.to_json()))
								.collect::<Vec<_>>()
								.join("\n")
						})
					})
			});
			let output = hover::Output { text };

			Ok(output)
		})
	}
}

fn marked_string_text(value: Value) -> Option<String> {
	// Read the text of a marked string, which is either a string or an object with a value.
	match value {
		Value::Object(object) => match object.get("value") {
			Some(Value::String(value)) => Some(value.clone()),
			_ => None,
		},
		Value::String(value) => Some(value),
		_ => None,
	}
}
