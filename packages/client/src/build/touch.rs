use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn touch_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/touch");
		let mut request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			);
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
