use crate::{self as tg, Client};
use bytes::Bytes;
use futures::{future, stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream};
use serde_with::serde_as;
use tangram_error::{error, Error, Result};
use tangram_http::{empty, full};
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<crate::util::SeekFromString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
	pub position: u64,
	#[serde_as(as = "crate::util::BytesBase64")]
	pub bytes: Bytes,
}

impl Client {
	pub async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/log?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self
			.send(request)
			.await
			.map_err(|source| error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
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
			.map_err(|source| error!(!source, "failed to read from the body"));
		let reader = Box::pin(StreamReader::new(stream.map_err(std::io::Error::other)));
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let output = tangram_sse::Decoder::new(reader)
			.map(|result| {
				let event = result.map_err(|source| error!(!source, "failed to read an event"))?;
				let chunk = serde_json::from_str(&event.data)
					.map_err(|source| error!(!source, "failed to deserialize the event data"))?;
				Ok::<_, Error>(chunk)
			})
			.take_until(stop)
			.boxed();
		Ok(Some(output))
	}

	pub async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/log");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		request = request.header(
			http::header::CONTENT_TYPE,
			mime::APPLICATION_OCTET_STREAM.to_string(),
		);
		let body = bytes;
		let body = full(body);
		let request = request
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}
}
