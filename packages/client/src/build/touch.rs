use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn touch_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::touch::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/touch");
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
