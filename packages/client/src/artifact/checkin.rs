use crate::{self as tg, util::serde::is_false};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use std::pin::pin;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub destructive: bool,

	pub path: tg::Path,
}

#[derive(Clone, Debug)]
pub enum Event {
	Progress(Progress),
	End(tg::artifact::Id),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Progress {
	pub count: tg::Progress,
	pub weight: tg::Progress,
}

impl tg::Artifact {
	pub async fn check_in<H>(handle: &H, arg: tg::artifact::checkin::Arg) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let stream = handle.check_in_artifact(arg).await?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if let Event::End(artifact) = event {
				return Ok(Self::with_id(artifact));
			}
		}
		Err(tg::error!("expected an artifact"))
	}
}

impl tg::Client {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>>> {
		let method = http::Method::POST;
		let uri = "/artifacts/checkin";
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
					let progress = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::artifact::checkin::Event::Progress(progress))
				},
				Some("end") => {
					let artifact = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(tg::artifact::checkin::Event::End(artifact))
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
