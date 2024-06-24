use crate::{self as tg, util::serde::is_false};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use std::{path::PathBuf, pin::pin};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub destructive: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub deterministic: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub locked: bool,

	pub path: PathBuf,
}

impl tg::Artifact {
	pub async fn check_in<H>(handle: &H, arg: tg::artifact::checkin::Arg) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let stream = handle.check_in_artifact(arg).await?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if let tg::Progress::End(artifact) = event {
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
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<tg::artifact::Id>>>> {
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
			event.try_into()
		});
		Ok(output)
	}
}
