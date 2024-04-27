use super::Id;
use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};
use futures::{future, stream, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use http_body_util::BodyStream;
use serde_with::serde_as;
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Arg {
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

impl tg::Build {
	pub async fn children<H>(
		&self,
		handle: &H,
		arg: tg::build::children::Arg,
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
		arg: tg::build::children::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Self>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		Ok(handle
			.try_get_build_children(self.id(), arg, None)
			.await?
			.map(|stream| {
				stream
					.map_ok(|chunk| {
						stream::iter(chunk.items.into_iter().map(tg::Build::with_id).map(Ok))
					})
					.try_flatten()
					.boxed()
			}))
	}

	pub async fn add_child<H>(&self, handle: &H, child: &Self) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let child_id = child.id();
		handle.add_build_child(id, child_id).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
	> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/children?{query}");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		let response = response.success().await?;
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
		let body = Outgoing::json(child_id.clone());
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
