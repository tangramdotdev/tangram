use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use tokio::io::AsyncRead;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Client {
	pub(crate) async fn export_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::export::Arg,
	) -> tg::Result<impl AsyncRead + Send + 'static> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/export");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let reader = response.reader();
		Ok(reader)
	}
}
