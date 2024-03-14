use crate as tg;
use crate::{value, Client, Value};
use derive_more::TryUnwrap;
use futures::{future, FutureExt};
use http_body_util::BodyExt;
use serde_with::serde_as;
use std::pin::pin;
use tangram_error::{error, Error, Result};
use tangram_util::http::{empty, full};

#[derive(Clone, Debug, serde::Deserialize, TryUnwrap)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
pub enum Outcome {
	Canceled,
	Failed(Error),
	Succeeded(Value),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, TryUnwrap)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[try_unwrap(ref)]
pub enum Data {
	Canceled,
	Failed(Error),
	Succeeded(value::Data),
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

impl Client {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/outcome?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.body(body)
			.map_err(|error| error!(source = error, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|error| error!(source = error, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
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
				.map_err(|error| error!(source = error, "failed to collect the response body"))?
				.to_bytes();
			let outcome = serde_json::from_slice(&bytes).map_err(|error| {
				error!(source = error, "failed to deserialize the response body")
			})?;
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

	pub async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/outcome");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let outcome = outcome.data(self).await?;
		let body = serde_json::to_vec(&outcome)
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
		let body = full(body);
		let request = request
			.body(body)
			.map_err(|error| error!(source = error, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|error| error!(source = error, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}
}
