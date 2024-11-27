use crate::{self as tg, util::serde::BytesBase64};
use bytes::Bytes;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize)]
pub struct Arg {
	pub kind: tg::object::Kind,
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::object::Id,
}

impl tg::Client {
	pub async fn post_object(
		&self,
		arg: tg::object::post::Arg,
	) -> tg::Result<tg::object::post::Output> {
		let method = http::Method::POST;
		let uri = "/objects";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.bytes(arg.bytes.clone())
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
