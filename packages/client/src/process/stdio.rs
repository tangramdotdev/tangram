use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::{BytesBase64, CommaSeparatedString},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(
	Clone,
	Copy,
	Default,
	Debug,
	PartialEq,
	Eq,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Stdio {
	#[default]
	#[tangram_serialize(id = 0)]
	Null,
	#[tangram_serialize(id = 1)]
	Log,
	#[tangram_serialize(id = 2)]
	Pipe,
	#[tangram_serialize(id = 3)]
	Pty,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Stream {
	#[tangram_serialize(id = 0)]
	Stdin,
	#[tangram_serialize(id = 1)]
	Stdout,
	#[tangram_serialize(id = 2)]
	Stderr,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Event {
	Chunk(Chunk),
	End,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

#[derive(Clone, Debug)]
pub enum OutputEvent {
	End,
	Stop,
}

impl tg::Client {
	pub async fn try_read_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::stdio::Event>> + Send + 'static>,
	> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stdin)
			.await
	}

	pub async fn write_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::OutputEvent>> + Send + 'static,
	> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stdin, stream)
			.await
	}

	pub async fn try_read_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::stdio::Event>> + Send + 'static>,
	> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stdout)
			.await
	}

	pub async fn write_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::OutputEvent>> + Send + 'static,
	> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stdout, stream)
			.await
	}

	pub async fn try_read_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::stdio::Event>> + Send + 'static>,
	> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stderr)
			.await
	}

	pub async fn write_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::OutputEvent>> + Send + 'static,
	> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stderr, stream)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<
		Option<impl futures::Stream<Item = tg::Result<tg::process::stdio::Event>> + Send + 'static>,
	> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/{stream}/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
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

	async fn write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stdio_stream: tg::process::stdio::Stream,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::process::stdio::OutputEvent>> + Send + use<>,
	> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/{stdio_stream}/write?{query}");
		let sse_stream = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(sse_stream)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
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
		Ok(stream)
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

impl TryFrom<OutputEvent> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: OutputEvent) -> Result<Self, Self::Error> {
		let event = match value {
			OutputEvent::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
			OutputEvent::Stop => tangram_http::sse::Event {
				event: Some("stop".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for OutputEvent {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("end") => Ok(Self::End),
			Some("stop") => Ok(Self::Stop),
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Null => write!(f, "null"),
			Self::Log => write!(f, "log"),
			Self::Pipe => write!(f, "pipe"),
			Self::Pty => write!(f, "pty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"null" => Ok(Self::Null),
			"log" => Ok(Self::Log),
			"pipe" => Ok(Self::Pipe),
			"pty" => Ok(Self::Pty),
			_ => Err(tg::error!(string = %s, "invalid stdio {s:?}")),
		}
	}
}

impl std::fmt::Display for Stream {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdin => write!(f, "stdin"),
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}

impl std::str::FromStr for Stream {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"stdin" => Ok(Self::Stdin),
			"stdout" => Ok(Self::Stdout),
			"stderr" => Ok(Self::Stderr),
			stream => Err(tg::error!(%stream, "unknown stream")),
		}
	}
}
