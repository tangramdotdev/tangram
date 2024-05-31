use crate as tg;
use bytes::Bytes;
use std::collections::BTreeSet;
use tangram_http::{incoming::response::Ext as _, Outgoing};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub bytes: Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub incomplete: BTreeSet<tg::object::Id>,
}

impl tg::Client {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		_transaction: Option<&()>,
	) -> tg::Result<tg::object::put::Output> {
		let method = http::Method::PUT;
		let uri = format!("/objects/{id}");
		let body = Outgoing::bytes(arg.bytes.clone());
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
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
