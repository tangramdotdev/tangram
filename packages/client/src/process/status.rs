use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	derive_more::IsVariant,
	derive_more::Unwrap,
	derive_more::TryUnwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Status {
	Created,
	Enqueued,
	Dequeued,
	Started,
	Finished,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Event {
	Status(Status),
	End,
}

impl tg::Process {
	pub async fn status<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_status(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_status<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		handle
			.try_get_process_status(self.id())
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}
}

impl tg::Client {
	pub async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let uri = format!("/processes/{id}/status");
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

impl std::fmt::Display for Status {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Created => write!(f, "created"),
			Self::Enqueued => write!(f, "enqueued"),
			Self::Dequeued => write!(f, "dequeued"),
			Self::Started => write!(f, "started"),
			Self::Finished => write!(f, "finished"),
		}
	}
}

impl std::str::FromStr for Status {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"created" => Ok(Self::Created),
			"enqueued" => Ok(Self::Enqueued),
			"dequeued" => Ok(Self::Dequeued),
			"started" => Ok(Self::Started),
			"finished" => Ok(Self::Finished),
			status => Err(tg::error!(%status, "invalid value")),
		}
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Status(status) => {
				let data = serde_json::to_string(&status)
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
				let status = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Status(status))
			},
			Some("end") => Ok(Self::End),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
