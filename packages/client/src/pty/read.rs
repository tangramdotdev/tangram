use crate as tg;
use futures::{Stream, TryStreamExt as _, future};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub master: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + use<>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/ptys/{id}/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			if matches!(response.status(), http::StatusCode::NOT_FOUND) {
				return Err(tg::error!(%id, "not found"));
			}
			let error = response
				.json()
				.await
				.map_err(|source| tg::error!(!source, "failed to parse error"))?;
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
		Ok(stream)
	}
}

impl TryFrom<tg::pty::Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: tg::pty::Event) -> Result<Self, Self::Error> {
		let event = match value {
			tg::pty::Event::Chunk(bytes) => {
				let data = data_encoding::BASE64.encode(&bytes);
				tangram_http::sse::Event {
					data,
					..Default::default()
				}
			},
			tg::pty::Event::Size(size) => {
				let data = serde_json::to_string(&size)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					event: Some("size".to_owned()),
					data,
					..Default::default()
				}
			},
			tg::pty::Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for tg::pty::Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			None => {
				let bytes = data_encoding::BASE64
					.decode(value.data.as_bytes())
					.map_err(|error| tg::error!(!error, "failed to decode the bytes"))?;
				Ok(Self::Chunk(bytes.into()))
			},
			Some("size") => {
				let size = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Size(size))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}
