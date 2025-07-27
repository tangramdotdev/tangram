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
	pub error: Option<tg::error::Data>,

	pub exit: u8,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	pub output: Option<tg::value::Data>,
}

#[derive(Clone, Debug)]
pub struct Wait {
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

impl Wait {
	pub fn into_output(self) -> tg::Result<tg::Value> {
		if let Some(error) = self.error {
			return Err(error);
		}
		match self.exit {
			0 => (),
			1..128 => {
				return Err(tg::error!("the process exited with code {}", self.exit));
			},
			128.. => {
				let signal = self.exit - 128;
				return Err(tg::error!("the process exited with signal {signal}"));
			},
		}
		let output = self.output.unwrap_or(tg::Value::Null);
		Ok(output)
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
			error: value.error.map(TryInto::try_into).transpose()?,
			exit: value.exit,
			output: value.output.map(TryInto::try_into).transpose()?,
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
