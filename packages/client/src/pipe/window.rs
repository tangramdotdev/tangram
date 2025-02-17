use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

use crate as tg;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub rows: u64,
	pub cols: u64,
}

impl tg::Client {
	pub async fn post_pipe_window(&self, id: &tg::pipe::Id, arg: Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/pipes/{id}/window");
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
		Ok(())
	}
}
