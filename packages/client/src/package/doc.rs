use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/doc");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
