use crate::{self as tg, prelude::*, util::serde::SeekFromNumberOrString};
use futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream};
use serde_with::serde_as;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum Event {
	Chunk(Chunk),
	End,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	pub data: Vec<tg::process::Id>,
}

impl tg::Process {
	pub async fn children<H>(
		&self,
		handle: &H,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<Self>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_children(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_children<H>(
		&self,
		handle: &H,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Self>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		Ok(handle
			.try_get_process_children(self.id(), arg)
			.await?
			.map(|stream| {
				stream
					.map_ok(|chunk| {
						stream::iter(
							chunk
								.data
								.into_iter()
								.map(|id| tg::Process::new(id, None, None, None, None))
								.map(Ok),
						)
					})
					.try_flatten()
					.boxed()
			}))
	}
}

impl tg::Client {
	pub async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/processes/{id}/children?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
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

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Chunk(chunk) => {
				let data = serde_json::to_string(&chunk)
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

impl TryFrom<tangram_http::sse::Event> for Event {
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
