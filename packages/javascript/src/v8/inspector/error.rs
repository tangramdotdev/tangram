use {super::protocol, tangram_client::prelude::*};

pub(super) const EXCEPTION_TO_ERROR_DATA_FUNCTION: &str = r#"
function(exception) {
	function format(value) {
		if (value === undefined) {
			return "undefined";
		}
		if (value && typeof value === "object" && typeof value.message === "string") {
			return value.message;
		}
		try {
			return String(value);
		} catch (_) {
			return "an exception occurred";
		}
	}
	let Tangram = globalThis.Tangram;
	if (Tangram && Tangram.Error && exception instanceof Tangram.Error) {
		try {
			return Tangram.Error.toData(exception);
		} catch (_) {}
	}
	let Error = globalThis.Error;
	if (typeof Error === "function" && exception instanceof Error) {
		return { message: format(exception) };
	}
	return { message: format(exception) };
}
"#;

pub(super) fn from_response(response: &protocol::Response, fallback: &str) -> tg::Error {
	let Ok(protocol::ResponseResult::Value(remote)) = protocol::response_result(response) else {
		return tg::error!("{fallback}");
	};
	let Some(value) = remote.value.as_ref() else {
		return tg::error!("{fallback}");
	};
	serde_json::from_value::<tg::error::Data>(value.clone())
		.ok()
		.and_then(|data| tg::error::Object::try_from_data(data).ok())
		.map_or_else(|| tg::error!("{fallback}"), tg::Error::with_object)
}

pub(super) fn format_exception_details(details: &protocol::ExceptionDetails) -> String {
	details.exception.as_ref().map_or_else(
		|| {
			details
				.text
				.clone()
				.unwrap_or_else(|| "exception".to_owned())
		},
		format_exception_remote_object,
	)
}

fn format_exception_remote_object(remote: &protocol::RemoteObject) -> String {
	if remote.subtype.as_deref() == Some("error")
		&& let Some(description) = remote.description.as_deref()
		&& let Some(first_line) = description.lines().next()
	{
		let first_line = first_line.strip_prefix("Uncaught ").unwrap_or(first_line);
		return first_line
			.split_once(": ")
			.map_or_else(|| first_line.to_owned(), |(_, message)| message.to_owned());
	}

	format_remote_value(remote)
}

fn format_remote_value(remote: &protocol::RemoteObject) -> String {
	if let Some(value) = &remote.unserializable_value {
		return value.clone();
	}

	if remote.subtype.as_deref() == Some("null") {
		return "null".to_owned();
	}

	if let Some(value) = &remote.value {
		return match value {
			serde_json::Value::Null => "null".to_owned(),
			serde_json::Value::Bool(value) => value.to_string(),
			serde_json::Value::Number(value) => value.to_string(),
			serde_json::Value::String(value) => value.clone(),
			value => value.to_string(),
		};
	}

	if let Some(description) = &remote.description {
		return description.clone();
	}

	remote.kind.as_deref().unwrap_or("<unknown>").to_owned()
}
