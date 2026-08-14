use {
	crate::prelude::*,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
	},
	serde::Deserialize as _,
	serde_with::serde_as,
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lease: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug)]
pub enum Event {
	Output(Output),
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Error>")]
	pub error: Option<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,

	pub exit: u8,

	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::unwrap_or_skip"
	)]
	pub output: Option<tg::value::Data>,
}

#[derive(Clone, Debug)]
pub struct Wait {
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

struct Error;

impl tg::Session {
	pub async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		let method = http::Method::POST;
		let path = format!("/processes/{id}/wait");
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
			})
			.boxed();
		let future = stream.boxed().try_last().map_ok(|option| {
			option.map(|output| {
				let Event::Output(output) = output;
				output
			})
		});
		Ok(Some(future.boxed()))
	}
}

impl Wait {
	pub(crate) fn inherit_location(&self, location: Option<&tg::Location>) {
		if let Some(error) = &self.error {
			error.state().inherit_location(location);
		}
		if let Some(output) = &self.output {
			output.inherit_location(location);
		}
	}

	pub(crate) fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		if let Some(error) = &self.error {
			error.state().inherit_tokens(tokens);
		}
		if let Some(output) = &self.output {
			output.inherit_tokens(tokens);
		}
	}

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

impl Wait {
	pub fn try_from_data(data: Output) -> tg::Result<Self> {
		let error = data
			.error
			.map(|either| match either {
				tg::Either::Left(data) => {
					let object = tg::error::Object::try_from_data(data)?;
					Ok::<_, tg::Error>(tg::Error::with_object(object))
				},
				tg::Either::Right(error) => Ok(tg::Error::with_referent(error)),
			})
			.transpose()?;
		Ok(Self {
			error,
			exit: data.exit,
			output: data.output.map(TryInto::try_into).transpose()?,
		})
	}

	#[must_use]
	pub fn to_data(&self) -> Output {
		Output {
			error: self
				.error
				.as_ref()
				.map(|error| error.to_data_or_id().map_right(|_| error.to_referent())),
			exit: self.exit,
			output: self.output.as_ref().map(tg::Value::to_data),
		}
	}
}

impl TryFrom<Output> for Wait {
	type Error = tg::Error;

	fn try_from(value: Output) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Output(output) => {
				let data = serde_json::to_string(&output)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
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
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Output(output))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			value => Err(tg::error!(?value, "invalid event")),
		}
	}
}

impl serde_with::SerializeAs<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>> for Error {
	fn serialize_as<S>(
		source: &tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
		serializer: S,
	) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match source {
			tg::Either::Left(data) => serde::Serialize::serialize(data, serializer),
			tg::Either::Right(referent) => serializer.collect_str(referent),
		}
	}
}

impl<'de> serde_with::DeserializeAs<'de, tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>
	for Error
{
	fn deserialize_as<D>(
		deserializer: D,
	) -> Result<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let value = tg::Either::<tg::error::Data, String>::deserialize(deserializer)?;
		let value = match value {
			tg::Either::Left(data) => tg::Either::Left(data),
			tg::Either::Right(value) => {
				let referent = value.parse().map_err(serde::de::Error::custom)?;
				tg::Either::Right(referent)
			},
		};

		Ok(value)
	}
}
