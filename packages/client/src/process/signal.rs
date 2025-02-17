use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

use crate as tg;

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub signal: i32,
}

impl tg::Client {
	pub(crate) async fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/processes/{id}/signal");
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
