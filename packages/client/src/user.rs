use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[debug("tg::user::Id(\"{_0}\")]")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct User {
	pub id: Id,
	pub email: String,
}

impl tg::Client {
	pub async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		let method = http::Method::GET;
		let uri = "/user";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::AUTHORIZATION, format!("Bearer {token}"))
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(output)
	}
}

impl From<tg::user::Id> for tg::Id {
	fn from(value: tg::user::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::user::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::User {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for tg::user::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}
