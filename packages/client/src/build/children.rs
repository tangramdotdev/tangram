use super::Id;
use crate::{
	self as tg,
	util::http::{empty, full},
};
use futures::{future, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use serde_with::serde_as;
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<crate::util::serde::SeekFromString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
	pub position: u64,
	pub items: Vec<Id>,
}

impl tg::Client {
	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/children?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let reader = StreamReader::new(
			BodyStream::new(response.into_body())
				.try_filter_map(|frame| future::ok(frame.into_data().ok()))
				.map_err(std::io::Error::other),
		);
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let output = tangram_sse::Decoder::new(reader)
			.map(|result| {
				let event =
					result.map_err(|source| tg::error!(!source, "failed to read an event"))?;
				let chunk = serde_json::from_str(&event.data).map_err(|source| {
					tg::error!(!source, "failed to deserialize the event data")
				})?;
				Ok::<_, tg::Error>(chunk)
			})
			.take_until(stop);
		Ok(Some(output))
	}

	pub async fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{build_id}/children");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		request = request.header(
			http::header::CONTENT_TYPE,
			mime::APPLICATION_JSON.to_string(),
		);
		let body = serde_json::to_vec(&child_id)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}
}
