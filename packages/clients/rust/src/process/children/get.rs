use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream},
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::SeekFromNumberOrString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::grant::Token>,
}

#[derive(Clone, Debug)]
pub enum Event {
	Chunk(Chunk),
	End,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	pub data: Vec<tg::process::data::Child>,
}

impl<O> tg::Process<O> {
	pub async fn children(
		&self,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::state::Child>> + Send + 'static> {
		let handle = tg::handle()?;
		self.children_with_handle(handle, arg).await
	}

	pub async fn children_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::state::Child>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_children_with_handle(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_children(
		&self,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::state::Child>> + Send + 'static>,
	> {
		let handle = tg::handle()?;
		self.try_get_children_with_handle(handle, arg).await
	}

	pub async fn try_get_children_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::state::Child>> + Send + 'static>,
	>
	where
		H: tg::Handle,
	{
		let token = self.token();
		let mut arg = arg;
		if arg.token.is_none() {
			arg.token = token.clone();
		}
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"getting the process children is not supported for unsandboxed processes"
			));
		};
		Ok(handle
			.try_get_process_children(id, arg)
			.await?
			.map(move |stream| {
				stream
					.map_ok(move |chunk| {
						let token = token.clone();
						stream::iter(chunk.data.into_iter().map(move |data| {
							let child = tg::process::state::Child::try_from_data(data)?;
							child.process.inherit_token(token.clone());
							Ok(child)
						}))
					})
					.try_flatten()
					.boxed()
			}))
	}
}

impl tg::Session {
	pub async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static + use<>,
		>,
	> {
		let method = http::Method::GET;
		let path = format!("/processes/{id}/children");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
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
			Event::Chunk(chunk) => {
				let data = serde_json::to_string(&chunk)
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
				let chunk = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Chunk(chunk))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}
