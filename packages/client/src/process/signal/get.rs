use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _},
	std::future,
	tangram_http::{request::builder::Ext as _, response::Ext as _, sse},
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Event {
	Signal(tg::process::Signal),
	End,
}

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Event>> + Send + 'static>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/signal?{query}");
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
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});
		Ok(Some(stream))
	}
}

impl TryFrom<Event> for sse::Event {
	type Error = tg::Error;
	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Signal(signal) => {
				let data = serde_json::to_string(&signal)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					..Default::default()
				}
			},
			Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<sse::Event> for Event {
	type Error = tg::Error;
	fn try_from(value: sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			None => {
				let signal = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Signal(signal))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}
