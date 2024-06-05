use crate as tg;
use bytes::Bytes;
use serde_with::serde_as;
use std::collections::BTreeSet;
use tangram_http::{incoming::response::Ext as _, Outgoing};

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "crate::util::serde::BytesBase64")]
	pub bytes: Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
	pub incomplete: BTreeSet<tg::object::Id>,
}

impl tg::Client {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
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
