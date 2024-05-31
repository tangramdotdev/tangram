use crate as tg;
use futures::{Stream, StreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub remote: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Event {
	Progress(Progress),
	End,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Progress {
	pub current_count: u64,
	pub total_count: u64,
}

impl tg::Build {
	pub async fn push<H>(
		&self,
		handle: &H,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static>
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
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(arg).unwrap();
		let uri = format!("/builds/{id}/push?{query}");
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
