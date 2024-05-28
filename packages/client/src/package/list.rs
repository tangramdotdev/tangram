use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub query: Option<String>,
	pub remote: Option<String>,
}

pub type Output = Vec<String>;

impl tg::Client {
	pub async fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(arg).unwrap();
		let uri = format!("/packages?{query}");
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
		let output = response.json().await?;
		Ok(output)
	}
}
