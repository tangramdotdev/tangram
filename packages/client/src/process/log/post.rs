use {
	crate::{self as tg},
	bytes::Bytes,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::BytesBase64,
};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,

	pub stream: tg::process::log::Stream,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Process {
	pub async fn post_log<H>(&self, handle: &H, arg: tg::process::log::post::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.post_process_log(id, arg).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/log");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
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
