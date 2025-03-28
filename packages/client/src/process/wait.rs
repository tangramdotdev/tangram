use crate as tg;
use futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, future};
use tangram_futures::stream::TryExt as _;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug)]
pub enum Event {
	Output(Output),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Error>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<tg::process::Exit>,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	pub output: Option<tg::value::Data>,

	pub status: tg::process::Status,
}

#[derive(Clone, Debug)]
pub struct Wait {
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub output: Option<tg::Value>,
	pub status: tg::process::Status,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Exit {
	Code { code: i32 },
	Signal { signal: i32 },
}

impl Exit {
	#[must_use]
	pub fn failed(&self) -> bool {
		match self {
			Self::Code { code } if *code != 0 => true,
			Self::Signal { .. } => true,
			Self::Code { .. } => false,
		}
	}
}

impl tg::Client {
	pub async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/wait");
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
			})
			.boxed();
		let future = stream.boxed().try_last().map_ok(|option| {
			option.map(|output| {
				let Event::Output(output) = output;
				output
			})
		});
		Ok(Some(future))
	}
}

impl TryFrom<Output> for Wait {
	type Error = tg::Error;

	fn try_from(value: Output) -> Result<Self, Self::Error> {
		Ok(Self {
			error: value.error,
			exit: value.exit,
			output: value.output.map(tg::Value::try_from).transpose()?,
			status: value.status,
		})
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Output(output) => {
				let data = serde_json::to_string(&output)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("output".into()),
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
			Some("output") => {
				let output = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Output(output))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Err(error)
			},
			value => Err(tg::error!(?value, "invalid event")),
		}
	}
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	use serde::Deserialize as _;
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
