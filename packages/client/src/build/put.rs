use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

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
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(Outgoing::json(arg))
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
