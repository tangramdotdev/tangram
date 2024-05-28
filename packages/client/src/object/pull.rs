use crate as tg;
use futures::{future, Stream, StreamExt as _, TryStreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub remote: Option<String>,
}

impl tg::Object {
	pub async fn pull<H>(
		&self,
		handle: &H,
		arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>> + Send + 'static>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let stream = handle.pull_object(&id, arg).await?.boxed();
		Ok(stream)
	}
}

impl tg::Client {
	pub async fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::Progress>> + Send + 'static> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/objects/{id}/pull?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response
			.sse()
			.and_then(|event| future::ready(serde_json::from_str(&event.data).map_err(Into::into)))
			.map_err(|source| tg::error!(!source, "failed to read an event"));
		Ok(output)
	}
}
