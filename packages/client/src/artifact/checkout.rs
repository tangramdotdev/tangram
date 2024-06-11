use crate as tg;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use std::pin::pin;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub bundle: bool,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub force: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
	#[serde(
		default = "crate::util::serde::true_",
		skip_serializing_if = "crate::util::serde::is_true"
	)]
	pub references: bool,
}
#[derive(Clone, Debug)]
pub enum Event {
	Progress(Progress),
	End(tg::Path),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Progress {
	pub count: tg::Progress,
	pub weight: tg::Progress,
}

impl tg::Artifact {
	pub async fn check_out<H>(&self, handle: &H, arg: Arg) -> tg::Result<tg::Path>
	where
		H: tg::Handle,
	{
		let id = self.id(handle).await?;
		let stream = handle.check_out_artifact(&id, arg).await?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if let Event::End(path) = event {
				return Ok(path);
			}
		}
		Err(tg::error!("checkout failed."))
	}
}

impl tg::Client {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkout::Event>>> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
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
				None | Some("progress") => {
					let progress = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::artifact::checkout::Event::Progress(progress))
				},
				Some("end") => {
					let artifact = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::artifact::checkout::Event::End(artifact))
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

impl Default for Arg {
	fn default() -> Self {
		Self {
			bundle: false,
			force: false,
			path: None,
			references: true,
		}
	}
}
