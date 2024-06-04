use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub locked: bool,
}

impl tg::Client {
	pub async fn check_package(
		&self,
		dependency: &tg::Dependency,
		arg: Arg,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/packages/{dependency}/check?{query}");
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
