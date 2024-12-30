use crate::{self as tg, handle::Ext as _};
use futures::{future, Future, StreamExt as _, TryStreamExt as _};
use tangram_futures::stream::TryStreamExt as _;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

pub use self::data::Data;

#[derive(
	Clone,
	Debug,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Outcome {
	Cancelation(Cancelation),
	Failure(Failure),
	Success(Success),
}

#[derive(Clone, Debug)]
pub struct Cancelation {
	pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Failure {
	pub error: tg::Error,
	pub value: Option<tg::Value>,
}

#[derive(Clone, Debug)]
pub struct Success {
	pub value: tg::Value,
}

pub mod data {
	use crate as tg;

	#[derive(
		Clone,
		Debug,
		derive_more::IsVariant,
		derive_more::TryUnwrap,
		derive_more::Unwrap,
		serde::Deserialize,
		serde::Serialize,
	)]
	#[serde(tag = "kind", rename_all = "snake_case")]
	#[try_unwrap(ref)]
	#[unwrap(ref)]
	pub enum Data {
		Cancelation(Cancelation),
		Failure(Failure),
		Success(Success),
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Cancelation {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub reason: Option<String>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Failure {
		pub error: tg::Error,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub value: Option<tg::value::Data>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Success {
		pub value: tg::value::Data,
	}
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl Outcome {
	#[must_use]
	pub fn retry(&self) -> tg::build::Retry {
		match self {
			Self::Cancelation(Cancelation { .. }) => tg::build::Retry::Canceled,
			Self::Failure(Failure { .. }) => tg::build::Retry::Failed,
			Self::Success(Success { .. }) => tg::build::Retry::Succeeded,
		}
	}

	pub fn into_result(self) -> tg::Result<tg::Value> {
		match self {
			Self::Cancelation(Cancelation { reason }) => {
				Err(tg::error!(?reason, "the build was canceled"))
			},
			Self::Failure(Failure { error, .. }) => Err(error),
			Self::Success(Success { value }) => Ok(value),
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::build::outcome::Data>
	where
		H: tg::Handle,
	{
		Ok(match self {
			Self::Cancelation(cancelation) => {
				tg::build::outcome::Data::Cancelation(tg::build::outcome::data::Cancelation {
					reason: cancelation.reason.clone(),
				})
			},
			Self::Failure(failure) => {
				let value = if let Some(ref value) = failure.value {
					Some(value.data(handle).await?)
				} else {
					None
				};
				tg::build::outcome::Data::Failure(tg::build::outcome::data::Failure {
					error: failure.error.clone(),
					value,
				})
			},
			Self::Success(success) => {
				tg::build::outcome::Data::Success(tg::build::outcome::data::Success {
					value: success.value.data(handle).await?,
				})
			},
		})
	}
}

impl tg::Build {
	pub async fn outcome<H>(&self, handle: &H) -> tg::Result<tg::build::Outcome>
	where
		H: tg::Handle,
	{
		self.get_outcome(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the outcome"))
	}

	pub async fn get_outcome<H>(&self, handle: &H) -> tg::Result<Option<tg::build::Outcome>>
	where
		H: tg::Handle,
	{
		self.try_get_outcome(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))?
			.await
	}

	pub async fn try_get_outcome<H>(
		&self,
		handle: &H,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	>
	where
		H: tg::Handle,
	{
		handle
			.try_get_build_outcome(self.id())
			.await
			.map(|option| option.map(futures::FutureExt::boxed))
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let outcome = self.outcome(handle).await?;
		match outcome {
			tg::build::Outcome::Cancelation(Cancelation { reason }) => {
				Err(tg::error!(?reason, "the build was canceled"))
			},
			tg::build::Outcome::Failure(Failure { error, .. }) => Err(error),
			tg::build::Outcome::Success(Success { value }) => Ok(value),
		}
	}
}

impl tg::Client {
	pub async fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let uri = format!("/builds/{id}/outcome");
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
				future::ready({
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					}
				})
			});
		let future = stream.boxed().try_last();
		Ok(Some(future))
	}
}

impl TryFrom<tg::build::outcome::Data> for Outcome {
	type Error = tg::Error;

	fn try_from(data: tg::build::outcome::Data) -> tg::Result<Self, Self::Error> {
		match data {
			tg::build::outcome::Data::Cancelation(cancelation) => {
				Ok(Outcome::Cancelation(Cancelation {
					reason: cancelation.reason,
				}))
			},
			tg::build::outcome::Data::Failure(failure) => {
				let value = if let Some(value) = failure.value {
					Some(value.try_into()?)
				} else {
					None
				};
				Ok(Outcome::Failure(Failure {
					error: failure.error,
					value,
				}))
			},
			tg::build::outcome::Data::Success(success) => Ok(Outcome::Success(Success {
				value: success.value.try_into()?,
			})),
		}
	}
}

impl TryFrom<tg::build::outcome::Data> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: tg::build::outcome::Data) -> Result<Self, Self::Error> {
		let data = serde_json::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
		let event = tangram_http::sse::Event {
			data,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for tg::build::outcome::Data {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			None => {
				let outcome = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(outcome)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl TryFrom<tangram_http::sse::Event> for Outcome {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		tg::build::outcome::Data::try_from(value)?.try_into()
	}
}
