use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub remote: Option<String>,
}

impl tg::Client {
	pub async fn try_start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> tg::Result<Option<bool>> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/start");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(Some(output))
	}
}
