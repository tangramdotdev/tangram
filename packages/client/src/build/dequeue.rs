use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub timeout: Option<std::time::Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::build::Id,
}

impl tg::Client {
	pub async fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
		_stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		let method = http::Method::POST;
		let uri = "/builds/dequeue";
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
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(Some(output))
	}
}
