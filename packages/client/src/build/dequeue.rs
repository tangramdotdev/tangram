use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

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
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(Outgoing::json(arg))
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(Some(output))
	}
}
