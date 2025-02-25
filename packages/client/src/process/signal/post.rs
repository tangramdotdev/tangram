use tangram_http::{request::builder::Ext as _, response::Ext as _};

use crate as tg;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub signal: tg::process::Signal,
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn post_process_signal(&self, id: &tg::process::Id, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let method = http::Method::GET;
		let uri = format!("/processes/{id}/signal");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return Err(response.json().await?);
		}
		Ok(())
	}
}
