use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Stored {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub subtree: bool,
}

impl Stored {
	pub fn merge(&mut self, other: &Self) {
		self.subtree = self.subtree || other.subtree;
	}
}

impl tg::Session {
	pub async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
		arg: tg::object::stored::Arg,
	) -> tg::Result<Option<tg::object::Stored>> {
		let method = http::Method::GET;
		let path = format!("/objects/{id}/stored");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let stored = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;

		Ok(Some(stored))
	}
}
