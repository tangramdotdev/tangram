use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn publish_package(&self, id: &tg::directory::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/packages";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&id)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = Outgoing::bytes(body);
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
