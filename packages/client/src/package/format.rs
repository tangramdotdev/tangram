use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/format");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
