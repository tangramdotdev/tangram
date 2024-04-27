use crate::{self as tg, util::http::full};
use either::Either;
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub name: String,
	#[serde(with = "either::serde_untagged")]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl tg::Client {
	pub async fn add_root(&self, arg: tg::root::add::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/roots";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let body = full(json);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}
}
