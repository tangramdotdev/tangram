use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub pty: tg::process::Pty,
	pub remote: Option<String>,
}

impl tg::Client {
	pub(crate) async fn try_put_process_pty(
		&self,
		id: &tg::process::Id,
		arg: Arg,
	) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/processes/{id}/pty");
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
