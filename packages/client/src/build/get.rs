use crate as tg;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::build::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	pub depth: u64,

	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcome: Option<tg::build::outcome::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcomes_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcomes_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcomes_weight: Option<u64>,

	pub retry: tg::build::Retry,

	pub status: tg::build::Status,

	pub target: tg::target::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub targets_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub targets_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub targets_weight: Option<u64>,

	#[serde_as(as = "Rfc3339")]
	pub created_at: time::OffsetDateTime,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub dequeued_at: Option<time::OffsetDateTime>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub started_at: Option<time::OffsetDateTime>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub finished_at: Option<time::OffsetDateTime>,
}

impl Output {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		let log = self.log.iter().map(|id| id.clone().into());
		let outcome = self
			.outcome
			.as_ref()
			.map(|outcome| {
				if let tg::build::outcome::Data::Succeeded(value) = outcome {
					value.children()
				} else {
					vec![]
				}
			})
			.into_iter()
			.flatten();
		let target = std::iter::once(self.target.clone().into());
		std::iter::empty()
			.chain(log)
			.chain(outcome)
			.chain(target)
			.collect()
	}
}

impl tg::Client {
	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/builds/{id}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
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
