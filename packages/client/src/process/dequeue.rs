use {
	crate::prelude::*,
	futures::{StreamExt as _, TryStreamExt as _, future},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub process: tg::process::Id,
}

impl tg::Client {
	pub async fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		let method = http::Method::POST;
		let uri = "/processes/dequeue";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
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
			.map_err(|source| tg::error!(!source, "failed to read an event"))
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

impl TryFrom<tg::process::dequeue::Output> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: tg::process::dequeue::Output) -> Result<Self, Self::Error> {
		let data = serde_json::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
		let event = tangram_http::sse::Event {
			data,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for tg::process::dequeue::Output {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			None => {
				let output = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(output)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
