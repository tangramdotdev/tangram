use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub items: Vec<tg::root::get::Output>,
}

impl tg::Client {
	pub async fn list_roots(&self, arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/roots?{query}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let request = request.empty().unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
