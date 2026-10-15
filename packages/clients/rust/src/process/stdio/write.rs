use {
	super::Stream,
	crate::prelude::*,
	futures::{TryStreamExt as _, stream::BoxStream},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

mod all;

pub(crate) use all::all;

pub mod stream;

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Data {
	#[tangram_serialize(id = 0)]
	Chunk(super::Chunk),
	#[tangram_serialize(id = 1)]
	End(super::End),
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub streams: Vec<Stream>,
}

/// The position through which the server has written, and whether stdin is closed.
#[derive(
	Clone,
	Copy,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Output {
	#[tangram_serialize(id = 0)]
	pub closed: bool,

	#[tangram_serialize(id = 1)]
	pub position: u64,
}

pub(crate) struct Input {
	pub chunk: super::Chunk,
	pub completion: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<O> tg::Process<O> {
	pub async fn write_stdio(
		&self,
		options: tg::process::stdio::write::Options,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> tg::Result<()> {
		let instance = tg::instance()?;
		self.write_stdio_with_instance(instance, options, input)
			.await
	}

	pub async fn write_stdio_with_instance<I>(
		&self,
		instance: &I,
		options: tg::process::stdio::write::Options,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> tg::Result<()>
	where
		I: tg::Instance,
	{
		let instance = self.instance_with_instance(instance);
		let instance = &instance;
		if self.id().is_left() {
			if options.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
				return Err(tg::error!("writing stdout or stderr is invalid"));
			}
			let mut stdin = self.stdin();
			let mut input = std::pin::pin!(input);
			while let Some(chunk) = input.try_next().await? {
				if chunk.stream != tg::process::stdio::Stream::Stdin {
					return Err(tg::error!("invalid process stdio stream"));
				}
				stdin.write_with_instance(instance, &chunk.bytes).await?;
			}
			stdin.close_with_instance(instance).await?;

			return Ok(());
		}

		if options.location.is_none() && self.location().is_none() {
			self.ensure_location_with_instance(instance).await?;
		}
		let id = self.id().unwrap_right();
		let arg = tg::process::stdio::write::stream::Arg {
			id: None,
			location: options.location.or_else(|| self.location()),
			process: id.clone(),
			streams: options.streams,
			tokens: self.tokens(),
		};
		instance.write_process_stdio_all(arg, input).await
	}
}

impl tg::Session {
	pub async fn try_write_process_stdio(
		&self,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::stdio::write::Output>>
			+ Send
			+ 'static
			+ use<>,
		>,
	> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let max_frame_size = self.client().sync.max_frame_size;
		let path = "/processes/stdio/write";
		let uri = Uri::builder().path(path).build().unwrap();
		let body = super::encode(input, max_frame_size);
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(uri)
			.header(http::header::ACCEPT, super::TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, super::TANGRAM_CONTENT_TYPE)
			.arg_with_tangram(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
		if content_type != Some(super::TANGRAM_CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = super::decode(response.into_body(), max_frame_size);

		Ok(Some(stream))
	}
}

impl TryFrom<Data> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Data) -> tg::Result<Self> {
		let data = serde_json::to_string(&value)
			.map_err(|error| tg::error!(!error, "failed to serialize the stdio data"))?;
		let event = Some("data".to_owned());
		Ok(Self {
			data,
			event,
			..Self::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for Data {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("data") => serde_json::from_str(&value.data)
				.map_err(|error| tg::error!(!error, "failed to deserialize the stdio data")),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data).map_err(|error| {
						tg::error!(!error, "failed to deserialize the stdio error")
					})?;
				Err(error.try_into()?)
			},
			_ => Err(tg::error!("invalid stdio message")),
		}
	}
}

impl TryFrom<Output> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Output) -> tg::Result<Self> {
		let data = serde_json::to_string(&value)
			.map_err(|error| tg::error!(!error, "failed to serialize the stdio output"))?;
		let event = Some("output".to_owned());
		Ok(Self {
			data,
			event,
			..Self::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for Output {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("output") => serde_json::from_str(&value.data)
				.map_err(|error| tg::error!(!error, "failed to deserialize the stdio output")),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data).map_err(|error| {
						tg::error!(!error, "failed to deserialize the stdio error")
					})?;
				Err(error.try_into()?)
			},
			_ => Err(tg::error!("invalid stdio message")),
		}
	}
}
