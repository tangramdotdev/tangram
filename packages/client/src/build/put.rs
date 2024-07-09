use crate::{self as tg, util::serde::is_false};
use serde_with::serde_as;
use std::collections::{BTreeMap, BTreeSet};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub id: tg::build::Id,
	pub children: Vec<tg::build::Id>,
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcome: Option<tg::build::outcome::Data>,
	pub retry: tg::build::Retry,
	pub status: tg::build::Status,
	pub target: tg::target::Id,
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub incomplete: Incomplete,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Incomplete {
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub children: BTreeMap<tg::build::Id, IncompleteChild>,
	#[serde(default, skip_serializing_if = "is_false")]
	pub log: bool,
	#[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
	pub outcome: BTreeSet<tg::object::Id>,
	#[serde(default, skip_serializing_if = "is_false")]
	pub target: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IncompleteChild {
	#[serde(default, skip_serializing_if = "is_false")]
	pub build: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub logs: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub outcomes: bool,
	#[serde(default, skip_serializing_if = "is_false")]
	pub targets: bool,
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
	pub async fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> tg::Result<tg::build::put::Output> {
		let method = http::Method::PUT;
		let uri = format!("/builds/{id}");
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
