use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/start");
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
