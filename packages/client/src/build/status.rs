use crate as tg;
use crate::Client;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream};
use serde_with::serde_as;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::http::{empty, full};
use tokio_util::io::StreamReader;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Status {
	Queued,
	Running,
	Finished,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

impl Client {
	pub async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/status?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let stream = BodyStream::new(response.into_body())
			.filter_map(|frame| async {
				match frame.map(http_body::Frame::into_data) {
					Ok(Ok(bytes)) => Some(Ok(bytes)),
					Err(e) => Some(Err(e)),
					Ok(Err(_frame)) => None,
				}
			})
			.map_err(|error| error.wrap("Failed to read from the body."));
		let reader = Box::pin(StreamReader::new(stream.map_err(std::io::Error::other)));
		let output = tangram_util::sse::Decoder::new(reader)
			.map(|result| {
				let event = result.wrap_err("Failed to read an event.")?;
				let chunk = serde_json::from_str(&event.data)
					.wrap_err("Failed to deserialize the event data.")?;
				Ok::<_, Error>(chunk)
			})
			.boxed();
		Ok(Some(output))
	}

	pub async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/status");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&status).wrap_err("Failed to serialize the body.")?;
		let body = full(body);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		Ok(())
	}
}
