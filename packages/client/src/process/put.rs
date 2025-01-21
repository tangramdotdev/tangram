use crate::{self as tg, util::serde::is_false};
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub id: tg::process::Id,
	pub children: Vec<tg::process::Id>,
	pub depth: u64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Error>,
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs: Option<tg::value::data::Array>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,
	pub retry: bool,
	pub status: tg::process::Status,
	pub command: tg::command::Id,
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

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands_complete: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub complete: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub logs_complete: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs_complete: bool,
}

impl Arg {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		let logs = self
			.logs
			.iter()
			.flat_map(|id| id.iter().map(|object| object.unwrap_object_ref().clone()));
		let output = self
			.output
			.as_ref()
			.map(tg::value::data::Data::children)
			.into_iter()
			.flatten();
		let command = std::iter::once(self.command.clone().into());
		logs.chain(output).chain(command).collect()
	}
}

impl tg::Client {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		let method = http::Method::PUT;
		let uri = format!("/processes/{id}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
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
