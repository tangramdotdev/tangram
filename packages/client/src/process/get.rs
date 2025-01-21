use crate as tg;
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	pub depth: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Error>,

	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs: Option<tg::value::data::Array>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_weight: Option<u64>,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_weight: Option<u64>,

	pub retry: bool,

	pub status: tg::process::Status,

	pub command: tg::command::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_weight: Option<u64>,

	#[serde_as(as = "Rfc3339")]
	pub created_at: time::OffsetDateTime,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub enqueued_at: Option<time::OffsetDateTime>,

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
		let logs = self
			.logs
			.iter()
			.flat_map(|item| item.iter().map(|object| object.unwrap_object_ref().clone()));
		let output = self
			.output
			.as_ref()
			.map(tg::value::data::Data::children)
			.into_iter()
			.flatten();
		let command = std::iter::once(self.command.clone().into());
		std::iter::empty()
			.chain(logs)
			.chain(output)
			.chain(command)
			.collect()
	}
}

impl tg::Client {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/processes/{id}");
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

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	use serde::Deserialize as _;
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
