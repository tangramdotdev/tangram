use {
	crate::prelude::*,
	futures::{StreamExt as _, TryStreamExt as _, future},
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub sandbox: tg::sandbox::Id,
	pub process: Option<tg::process::Id>,
	pub process_token: Option<String>,
	pub token: Option<String>,
}

impl tg::Session {
	pub async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let method = http::Method::POST;
		let uri = "/sandboxes/dequeue";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = response
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
			.and_then(|event| {
				future::ready({
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					}
				})
			});
		let Some(output) = stream.boxed().try_last().await? else {
			return Ok(None);
		};
		Ok(Some(output))
	}
}

impl TryFrom<tg::sandbox::queue::Output> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: tg::sandbox::queue::Output) -> Result<Self, Self::Error> {
		let data = serde_json::to_string(&value)
			.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
		let event = tangram_http::sse::Event {
			data,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for tg::sandbox::queue::Output {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			None => {
				let output = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(output)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
