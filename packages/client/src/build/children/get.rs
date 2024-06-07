use crate as tg;
use futures::{stream, Stream, StreamExt as _, TryStreamExt as _};
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<crate::util::serde::SeekFromString>")]
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

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	pub data: Vec<tg::build::Id>,
}

impl tg::Build {
	pub async fn children<H>(
		&self,
		handle: &H,
		arg: tg::build::children::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<Self>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_children(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_children<H>(
		&self,
		handle: &H,
		arg: tg::build::children::get::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Self>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		Ok(handle
			.try_get_build_children(self.id(), arg)
			.await?
			.map(|stream| {
				stream
					.map_ok(|chunk| {
						stream::iter(chunk.data.into_iter().map(tg::Build::with_id).map(Ok))
					})
					.try_flatten()
					.boxed()
			}))
	}
}

impl tg::Client {
	pub async fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::get::Event>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/children?{query}");
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
				None | Some("chunk") => {
					let chunk = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::build::children::get::Event::Chunk(chunk))
				},
				Some("end") => Ok(tg::build::children::get::Event::End),
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
