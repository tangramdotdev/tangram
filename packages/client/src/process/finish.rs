use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	pub exit: u8,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Process {
	pub async fn finish<H>(&self, handle: &H, arg: tg::process::finish::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.finish_process(id, arg).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/finish");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
