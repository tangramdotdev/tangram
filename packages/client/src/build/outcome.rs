use crate as tg;
use futures::{future, FutureExt as _};
use http_body_util::BodyExt as _;
use serde_with::serde_as;
use std::pin::pin;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
pub enum Outcome {
	Canceled,
	Failed(tg::Error),
	Succeeded(tg::Value),
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[try_unwrap(ref)]
pub enum Data {
	Canceled,
	Failed(tg::Error),
	Succeeded(tg::value::Data),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

impl Outcome {
	#[must_use]
	pub fn retry(&self) -> tg::build::Retry {
		match self {
			Self::Canceled => tg::build::Retry::Canceled,
			Self::Failed(_) => tg::build::Retry::Failed,
			Self::Succeeded(_) => tg::build::Retry::Succeeded,
		}
	}

	pub fn into_result(self) -> tg::Result<tg::Value> {
		match self {
			Self::Canceled => Err(tg::error!("the build was canceled")),
			Self::Failed(error) => Err(error),
			Self::Succeeded(value) => Ok(value),
		}
	}

	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<tg::build::outcome::Data>
	where
		H: tg::Handle,
	{
		Ok(match self {
			Self::Canceled => tg::build::outcome::Data::Canceled,
			Self::Failed(error) => tg::build::outcome::Data::Failed(error.clone()),
			Self::Succeeded(value) => {
				tg::build::outcome::Data::Succeeded(value.data(handle, transaction).await?)
			},
		})
	}
}

impl tg::Build {
	pub async fn outcome<H>(&self, handle: &H) -> tg::Result<tg::build::Outcome>
	where
		H: tg::Handle,
	{
		self.get_outcome(handle, tg::build::outcome::Arg::default())
			.await?
			.ok_or_else(|| tg::error!("failed to get the outcome"))
	}

	pub async fn get_outcome<H>(
		&self,
		handle: &H,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<Option<tg::build::Outcome>>
	where
		H: tg::Handle,
	{
		self.try_get_outcome(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_outcome<H>(
		&self,
		handle: &H,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<Option<Option<tg::build::Outcome>>>
	where
		H: tg::Handle,
	{
		handle.try_get_build_outcome(self.id(), arg, None).await
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let outcome = self.outcome(handle).await?;
		match outcome {
			tg::build::Outcome::Canceled => Err(tg::error!("the build was canceled")),
			tg::build::Outcome::Failed(error) => Err(error),
			tg::build::Outcome::Succeeded(value) => Ok(value),
		}
	}
}

impl tg::Client {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/outcome?{query}");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let outcome = async move {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let outcome = serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
			Ok(outcome)
		};
		let stop = stop.map(|()| Ok(None));
		let outcome = match future::try_select(pin!(outcome), pin!(stop)).await {
			Ok(future::Either::Left((outcome, _)) | future::Either::Right((outcome, _))) => outcome,
			Err(future::Either::Left((error, _)) | future::Either::Right((error, _))) => {
				return Err(error)
			},
		};
		Ok(Some(outcome))
	}
}

impl TryFrom<tg::build::outcome::Data> for Outcome {
	type Error = tg::Error;

	fn try_from(data: tg::build::outcome::Data) -> tg::Result<Self, Self::Error> {
		match data {
			tg::build::outcome::Data::Canceled => Ok(Outcome::Canceled),
			tg::build::outcome::Data::Failed(error) => Ok(Outcome::Failed(error)),
			tg::build::outcome::Data::Succeeded(value) => Ok(Outcome::Succeeded(value.try_into()?)),
		}
	}
}
