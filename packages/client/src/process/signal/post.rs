use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	pub signal: tg::process::Signal,
}

impl tg::Client {
	pub async fn post_process_signal(&self, id: &tg::process::Id, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/signal");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return Err(response.json().await?);
		}
		Ok(())
	}
}
