use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn remove_root(&self, name: &str) -> tg::Result<()> {
		let method = http::Method::DELETE;
		let name = urlencoding::encode(name);
		let uri = format!("/roots/{name}");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
