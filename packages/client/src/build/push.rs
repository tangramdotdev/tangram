use crate as tg;
use futures::{Stream, StreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub logs: bool,
	pub outcomes: bool,
	pub recursive: bool,
	pub remote: String,
	pub targets: bool,
}

impl tg::Build {
	pub async fn push<H>(
		&self,
		handle: &H,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let stream = handle.push_build(id, arg).await?;
		Ok(stream.boxed())
	}
}

impl tg::Client {
	pub async fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/push");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.sse().map(|result| {
			let event = result.map_err(|source| tg::error!(!source, "failed to read an event"))?;
			event.try_into()
		});
		Ok(output)
	}
}
