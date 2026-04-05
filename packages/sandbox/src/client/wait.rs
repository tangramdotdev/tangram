use {
	super::Client,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::future::Future,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug)]
pub enum Event {
	Output(Output),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub status: u8,
}

impl Client {
	pub async fn wait(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<impl Future<Output = tg::Result<Option<Output>>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/wait");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
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
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();
		let future = stream.boxed().try_last().map_ok(|option: Option<Event>| {
			option.map(|event| {
				let Event::Output(output) = event;
				output
			})
		});
		Ok(future)
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		match value {
			Event::Output(output) => {
				let data = serde_json::to_string(&output)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				Ok(tangram_http::sse::Event {
					data,
					event: Some("output".into()),
					..Default::default()
				})
			},
		}
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("output") => {
				let output = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Output(output))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Err(error)
			},
			value => Err(tg::error!(?value, "invalid event")),
		}
	}
}
