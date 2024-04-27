use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub id: tg::build::Id,
	pub children: Vec<tg::build::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcome: Option<tg::build::outcome::Data>,
	pub retry: tg::build::Retry,
	pub status: tg::build::Status,
	pub target: tg::target::Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub weight: Option<u64>,
	#[serde(with = "time::serde::rfc3339")]
	pub created_at: time::OffsetDateTime,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub dequeued_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub started_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub finished_at: Option<time::OffsetDateTime>,
}

impl Arg {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		let log = self.log.iter().map(|id| id.clone().into());
		let outcome = self
			.outcome
			.as_ref()
			.map(|outcome| {
				outcome
					.try_unwrap_succeeded_ref()
					.ok()
					.map(tg::value::Data::children)
					.unwrap_or_default()
			})
			.into_iter()
			.flatten();
		let target = std::iter::once(self.target.clone().into());
		log.chain(outcome).chain(target).collect()
	}
}

impl tg::Client {
	pub async fn put_build(&self, id: &tg::build::Id, arg: tg::build::put::Arg) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/builds/{id}");
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
