use {
	super::Stream,
	crate::prelude::*,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	pub streams: Vec<Stream>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<tg::grant::Token>,
}

#[derive(Clone, Debug)]
pub enum Event {
	End,
	Stop,
	Write(usize),
}

impl tg::Session {
	pub async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::stdio::write::Event>>
			+ Send
			+ 'static
			+ use<>,
		>,
	> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let method = http::Method::POST;
		let path = format!("/processes/{id}/stdio");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream = stream.map(
			|result: tg::Result<tg::process::stdio::read::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(stream)
			.unwrap();
		let response = self
			.send(request)
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

impl<O> tg::Process<O> {
	pub async fn write_stdio_all<H>(
		&self,
		handle: &H,
		mut arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if self.id().is_left() {
			if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
				return Err(tg::error!("writing stdout or stderr is invalid"));
			}
			let mut stdin = self.stdin();
			let mut input = std::pin::pin!(input);
			while let Some(event) = input.try_next().await? {
				match event {
					tg::process::stdio::read::Event::Chunk(chunk) => {
						if chunk.stream != tg::process::stdio::Stream::Stdin {
							return Err(tg::error!("invalid process stdio stream"));
						}
						stdin.write_with_handle(handle, &chunk.bytes).await?;
					},
					tg::process::stdio::read::Event::End => {
						stdin.close_with_handle(handle).await?;
						break;
					},
				}
			}
			return Ok(());
		}

		if arg.location.is_none() {
			self.ensure_location_with_handle(handle).await?;
			arg.location = self.location();
		}
		if arg.token.is_none() {
			arg.token = self.token();
		}
		let id = self.id().unwrap_right();
		handle.write_process_stdio_all(id, arg, input).await
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
			Event::Stop => tangram_http::sse::Event {
				event: Some("stop".to_owned()),
				..Default::default()
			},
			Event::Write(len) => {
				let data = serde_json::to_string(&len)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("write".to_owned()),
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("end") => Ok(Self::End),
			Some("stop") => Ok(Self::Stop),
			Some("write") => {
				let len = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Write(len))
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
