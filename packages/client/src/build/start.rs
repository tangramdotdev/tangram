use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn try_start_build(&self, id: &tg::build::Id) -> tg::Result<Option<bool>> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/start");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(Some(output))
	}
}
