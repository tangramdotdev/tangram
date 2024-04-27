use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		let method = http::Method::GET;
		let uri = "/runtimes/js/doc";
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
