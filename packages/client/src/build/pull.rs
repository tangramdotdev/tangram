use crate as tg;
use futures::{Stream, StreamExt as _};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

pub type Arg = super::push::Arg;

impl tg::Build {
	pub async fn pull<H>(
		&self,
		handle: &H,
		arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let stream = handle.pull_build(id, arg).await?;
		Ok(stream.boxed())
	}
}

impl tg::Client {
	pub async fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/pull");
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
				None => {
					let report = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::Progress::Report(report))
				},
				Some("begin") => Ok(tg::Progress::Begin),
				Some("end") => Ok(tg::Progress::End(())),
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
