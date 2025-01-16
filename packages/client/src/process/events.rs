use crate as tg;
use crate::handle::Ext as _;
use bytes::Bytes;
use futures::{future, Stream, TryStreamExt};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind")]
pub enum Event {
	Status(tg::build::Status),
	Stderr(Bytes),
	Stdout(Bytes),
	End,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub event: Event,
}

impl tg::Process {
	pub async fn send_event<H>(&self, handle: &H, arg: tg::process::events::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.try_send_process_event(id, arg).await?;
		Ok(())
	}

	pub async fn events<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::events::Event>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_events(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_events<H>(
		&self,
		handle: &H,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::events::Event>> + Send + 'static>,
	>
	where
		H: tg::Handle,
	{
		handle
			.try_get_process_events(self.id())
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}
}

impl tg::Client {
	pub async fn try_send_process_event(
		&self,
		id: &tg::process::Id,
		arg: tg::process::events::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/event");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}

	pub async fn try_get_process_event_stream(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::events::Event>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let uri = format!("/process/{id}/events");
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
		todo!()
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		todo!()
	}
}
