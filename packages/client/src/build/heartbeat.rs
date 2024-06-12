use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub stop: bool,
}

impl tg::Build {
	pub async fn heartbeat<H>(
		&self,
		handle: &H,
		arg: tg::build::heartbeat::Arg,
	) -> tg::Result<tg::build::heartbeat::Output>
	where
		H: tg::Handle,
	{
		handle.heartbeat_build(self.id(), arg).await
	}
}

impl tg::Client {
	pub async fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/heartbeat");
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
		let output = response.json().await?;
		Ok(output)
	}
}
