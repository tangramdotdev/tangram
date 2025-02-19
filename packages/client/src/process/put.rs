use crate::{self as tg, util::serde::is_false};
use itertools::Itertools as _;
use serde_with::serde_as;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_http::{request::builder::Ext as _, response::Ext as _};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cacheable: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<tg::process::Id>>,

	pub command: tg::command::Id,

	#[serde_as(as = "Rfc3339")]
	pub created_at: time::OffsetDateTime,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub dequeued_at: Option<time::OffsetDateTime>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub enqueued_at: Option<time::OffsetDateTime>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub env: Option<BTreeMap<String, String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Error>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<tg::process::Exit>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub finished_at: Option<time::OffsetDateTime>,

	pub host: String,

	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub started_at: Option<time::OffsetDateTime>,

	pub status: tg::process::Status,
}

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
		let log = self.log.iter().cloned().map_into();
		let output = self
			.output
			.as_ref()
			.map(tg::value::data::Data::children)
			.into_iter()
			.flatten();
		let command = std::iter::once(self.command.clone().into());
		log.chain(output).chain(command).collect()
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
