use {
	super::{Chunk, Stream},
	crate::prelude::*,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{CommaSeparatedString, SeekFromNumberOrString},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(alias = "remote", default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	pub streams: Vec<Stream>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Event {
	Chunk(Chunk),
	End,
}

impl tg::Client {
	pub async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
		>,
	> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let method = http::Method::GET;
		let path = format!("/processes/{id}/stdio");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
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

impl<O> tg::Process<O> {
	pub async fn try_read_stdio_all<H>(
		&self,
		handle: &H,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>>
	where
		H: tg::Handle,
	{
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}

		if self.pid.is_some() {
			let mut streams = Vec::new();
			for stream in arg.streams {
				match stream {
					tg::process::stdio::Stream::Stdin => {
						return Err(tg::error!("reading stdin is invalid"));
					},
					tg::process::stdio::Stream::Stdout => {
						let handle = handle.clone();
						let stdout = self.stdout();
						let stream = stream::try_unfold(
							(handle, stdout),
							|(handle, mut stdout)| async move {
								let Some(bytes) = stdout.read_with_handle(&handle).await? else {
									return Ok(None);
								};
								let event = tg::process::stdio::read::Event::Chunk(
									tg::process::stdio::Chunk {
										bytes,
										position: None,
										stream: tg::process::stdio::Stream::Stdout,
									},
								);
								Ok(Some((event, (handle, stdout))))
							},
						);
						streams.push(stream.boxed());
					},
					tg::process::stdio::Stream::Stderr => {
						let handle = handle.clone();
						let stderr = self.stderr();
						let stream = stream::try_unfold(
							(handle, stderr),
							|(handle, mut stderr)| async move {
								let Some(bytes) = stderr.read_with_handle(&handle).await? else {
									return Ok(None);
								};
								let event = tg::process::stdio::read::Event::Chunk(
									tg::process::stdio::Chunk {
										bytes,
										position: None,
										stream: tg::process::stdio::Stream::Stderr,
									},
								);
								Ok(Some((event, (handle, stderr))))
							},
						);
						streams.push(stream.boxed());
					},
				}
			}
			let stream = futures::stream::select_all(streams).chain(stream::once(future::ok(
				tg::process::stdio::read::Event::End,
			)));
			return Ok(Some(stream.boxed()));
		}

		let Some(stream) = handle.try_read_process_stdio_all(self.id(), arg).await? else {
			return Ok(None);
		};

		Ok(Some(stream.boxed()))
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
