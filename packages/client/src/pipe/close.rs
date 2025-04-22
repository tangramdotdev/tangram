use crate as tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn close_pipe(&self, id: &tg::pipe::Id, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/pipes/{id}/close?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
