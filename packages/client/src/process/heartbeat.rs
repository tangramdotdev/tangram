use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub status: tg::process::Status,
}

impl tg::Process {
	pub async fn heartbeat<H>(
		&self,
		handle: &H,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output>
	where
		H: tg::Handle,
	{
		handle.heartbeat_process(self.id(), arg).await
	}
}

impl tg::Client {
	pub async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/heartbeat");
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
