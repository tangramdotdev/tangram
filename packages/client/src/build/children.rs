use super::Id;
use crate::{self as tg, Client};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream};
use serde_with::serde_as;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::http::{empty, full};
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub position: Option<u64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
	pub position: u64,
	pub data: Vec<Id>,
}

impl Client {
	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/children?{search_params}");
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

	pub async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{build_id}/children");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		request = request.header(
			http::header::CONTENT_TYPE,
			mime::APPLICATION_JSON.to_string(),
		);
		let body = serde_json::to_vec(&child_id).wrap_err("Failed to serialize the body.")?;
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
