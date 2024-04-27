use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub parent: Option<tg::build::Id>,
	pub remote: bool,
	pub retry: tg::build::Retry,
	pub target: tg::target::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::build::Id,
}

impl tg::Client {
	pub async fn create_build(
		&self,
		arg: tg::build::create::Arg,
	) -> tg::Result<tg::build::create::Output> {
		let method = http::Method::POST;
		let uri = "/builds";
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
		let body = Outgoing::json(arg);
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
