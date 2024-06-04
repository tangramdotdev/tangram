use crate as tg;
use futures::{Future, TryFutureExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

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

	pub async fn data<H>(&self, handle: &H) -> tg::Result<tg::build::outcome::Data>
	where
		H: tg::Handle,
	{
		Ok(match self {
			Self::Canceled => tg::build::outcome::Data::Canceled,
			Self::Failed(error) => tg::build::outcome::Data::Failed(error.clone()),
			Self::Succeeded(value) => {
				tg::build::outcome::Data::Succeeded(value.data(handle).await?)
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
			tg::build::Outcome::Canceled => Err(tg::error!("the build was canceled")),
			tg::build::Outcome::Failed(error) => Err(error),
			tg::build::Outcome::Succeeded(value) => Ok(value),
		}
	}
}

impl tg::Client {
	pub async fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>>>> {
		let method = http::Method::GET;
		let uri = format!("/builds/{id}/outcome");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
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
		let output = response.optional_json().err_into();
		Ok(Some(output))
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
