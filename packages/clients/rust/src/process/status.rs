use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _, future},
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	derive_more::Display,
	derive_more::FromStr,
	derive_more::IsVariant,
	derive_more::Unwrap,
	derive_more::TryUnwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Status {
	Started,
	Finished,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::grant::Token>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Event {
	Status(Status),
	End,
}

impl<O> tg::Process<O> {
	pub async fn status(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static> {
		let handle = tg::handle()?;
		self.status_with_handle(handle).await
	}

	pub async fn status_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_status_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_status(
		&self,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>> {
		let handle = tg::handle()?;
		self.try_get_status_with_handle(handle).await
	}

	pub async fn try_get_status_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		let arg = tg::process::status::Arg {
			location: self.location(),
			timeout: None,
			token: self.token(),
		};
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"getting the process status is not supported for unsandboxed processes"
			));
		};
		handle
			.try_get_process_status(id, arg)
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}
}

impl tg::Session {
	pub async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static + use<>>,
	> {
		let method = http::Method::GET;
		let path = format!("/processes/{id}/status");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
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
			Event::Status(status) => {
				let data = serde_json::to_string(&status)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
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
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Status(status))
			},
			Some("end") => Ok(Self::End),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
