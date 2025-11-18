use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{Stream, TryStreamExt as _, stream},
	http_body_util::BodyStream,
	num::ToPrimitive as _,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{BytesBase64, SeekFromNumberOrString, is_default},
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub blob: tg::blob::Id,

	#[serde(default, flatten, skip_serializing_if = "is_default")]
	pub options: Options,
}

#[serde_as]
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Options {
	#[serde_as(as = "serde_with::PickFirst<(_, Option<serde_with::DisplayFromStr>)>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde_as(as = "serde_with::PickFirst<(_, Option<serde_with::DisplayFromStr>)>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,
}

#[derive(Debug)]
pub enum Event {
	Chunk(Chunk),
	End,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

impl tg::Client {
	pub async fn try_read_blob_stream(
		&self,
		arg: Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
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
		let position = response
			.headers()
			.get("x-tg-position")
			.map(|v| v.to_str().unwrap().parse::<u64>())
			.transpose()
			.map_err(|_| tg::error!("expected an integer"))?;
		let frames = BodyStream::new(response.into_body());
		let stream = stream::try_unfold(
			(position, frames),
			|(mut position, mut frames)| async move {
				let Some(frame) = frames
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to read body"))?
				else {
					return Ok(None);
				};
				let event = match frame.into_data() {
					Ok(bytes) => {
						let position = position
							.as_mut()
							.ok_or_else(|| tg::error!("expected a position"))?;
						let chunk = Chunk {
							position: *position,
							bytes,
						};
						*position += chunk.bytes.len().to_u64().unwrap();
						tg::read::Event::Chunk(chunk)
					},
					Err(error) => {
						let trailers = error
							.into_trailers()
							.map_err(|_| tg::error!("expected trailers"))?;
						let event = trailers
							.get("x-tg-event")
							.ok_or_else(|| tg::error!("missing event"))?
							.to_str()
							.map_err(|source| tg::error!(!source, "invalid event"))?;
						match event {
							"end" => tg::read::Event::End,
							"error" => {
								let data = trailers
									.get("x-tg-data")
									.ok_or_else(|| tg::error!("missing data"))?
									.to_str()
									.map_err(|source| tg::error!(!source, "invalid data"))?;
								let error = serde_json::from_str(data).map_err(|source| {
									tg::error!(!source, "failed to deserialize the header value")
								})?;
								return Err(error);
							},
							_ => {
								return Err(tg::error!(%event, "unknown event"));
							},
						}
					},
				};
				Ok(Some((event, (position, frames))))
			},
		);
		Ok(Some(stream))
	}
}

impl Arg {
	#[must_use]
	pub fn with_blob(blob: tg::blob::Id) -> Self {
		Self {
			blob,
			options: tg::read::Options::default(),
		}
	}
}

impl TryFrom<tg::read::Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: tg::read::Event) -> Result<Self, Self::Error> {
		let event = match value {
			tg::read::Event::Chunk(chunk) => {
				let data = serde_json::to_string(&chunk)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					..Default::default()
				}
			},
			tg::read::Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for tg::read::Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			None => {
				let chunk = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Chunk(chunk))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}
