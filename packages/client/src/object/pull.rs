use crate as tg;
use futures::{Stream, StreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub remote: String,
}

pub type Event = super::push::Event;

pub type Progress = super::push::Progress;

impl tg::Object {
	pub async fn pull<H>(
		&self,
		handle: &H,
		arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static>
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
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/pull");
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
			match event.event.as_deref() {
				None | Some("data") => {
					let data = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(data)
				},
				Some("error") => {
					let error = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					Err(error)
				},
				_ => Err(tg::error!("invalid event")),
			}
		});
		Ok(output)
	}
}
