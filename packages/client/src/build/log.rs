use crate::{self as tg, Client};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream};
use serde_with::serde_as;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::http::{empty, full};
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetArg {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub position: Option<u64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
	pub position: u64,
	#[serde(
		deserialize_with = "deserialize_chunk_bytes",
		serialize_with = "serialize_chunk_bytes"
	)]
	pub bytes: Bytes,
}

impl Client {
	pub async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
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
			.wrap_err("Failed to create the request.")?;
		let response = self
			.send(request)
			.await
			.wrap_err("Failed to send the request.")?;
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

fn serialize_chunk_bytes<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where
	S: serde::Serializer,
{
	let string = data_encoding::BASE64.encode(value);
	serializer.serialize_str(&string)
}

fn deserialize_chunk_bytes<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
where
	D: serde::Deserializer<'de>,
{
	struct Visitor;

	impl<'de> serde::de::Visitor<'de> for Visitor {
		type Value = Bytes;

		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("a base64 encoded string")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			let bytes = data_encoding::BASE64
				.decode(value.as_bytes())
				.map_err(|_| serde::de::Error::custom("invalid string"))?
				.into();
			Ok(bytes)
		}
	}

	deserializer.deserialize_any(Visitor)
}
