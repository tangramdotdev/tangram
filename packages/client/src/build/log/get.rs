use crate::{
	self as tg,
	handle::Ext as _,
	util::serde::{BytesBase64, SeekFromString},
};
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum Event {
	Chunk(Chunk),
	End,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

impl tg::Build {
	pub async fn log<H>(
		&self,
		handle: &H,
		arg: tg::build::log::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::log::get::Chunk>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_log(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_log<H>(
		&self,
		handle: &H,
		arg: tg::build::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::log::get::Chunk>> + Send + 'static>,
	>
	where
		H: tg::Handle,
	{
		handle
			.try_get_build_log(self.id(), arg)
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}
}

impl tg::Client {
	pub async fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::log::get::Event>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/log?{query}");
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
		let output = response.sse().map(|result| {
			let event = result.map_err(|source| tg::error!(!source, "failed to read an event"))?;
			match event.event.as_deref() {
				None => {
					let chunk = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::build::log::get::Event::Chunk(chunk))
				},
				Some("end") => Ok(tg::build::log::get::Event::End),
				Some("error") => {
					let error = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					Err(error)
				},
				_ => Err(tg::error!("invalid event")),
			}
		});
		Ok(Some(output))
	}
}
