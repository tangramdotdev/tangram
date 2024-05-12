use crate as tg;
use futures::{future, stream, Stream, StreamExt as _, TryStreamExt as _};
use serde_with::serde_as;
use tangram_http::{incoming::ResponseExt as _, outgoing::RequestBuilderExt as _, Outgoing};

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
	pub items: Vec<tg::build::Id>,
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
			.try_get_build_children(self.id(), arg)
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
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
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
		let output = response
			.sse()
			.and_then(|event| future::ready(serde_json::from_str(&event.data).map_err(Into::into)))
			.map_err(|source| tg::error!(!source, "failed to read an event"));
		Ok(Some(output))
	}

	pub async fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{build_id}/children");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(Outgoing::json(child_id.clone()))
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
