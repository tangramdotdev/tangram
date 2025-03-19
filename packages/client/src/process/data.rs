use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use itertools::Itertools as _;
use serde_with::serde_as;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
};
use time::format_description::well_known::Rfc3339;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
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

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::process::data::Mount>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub started_at: Option<time::OffsetDateTime>,

	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<tg::process::Io>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::process::Io>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<tg::process::Io>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<Rfc3339>")]
	pub touched_at: Option<time::OffsetDateTime>,
}

impl Data {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		let logs = self.log.iter().cloned().map_into();
		let output = self
			.output
			.as_ref()
			.map(tg::value::data::Data::children)
			.into_iter()
			.flatten();
		let command = std::iter::once(self.command.clone().into());
		let mounts = self
			.mounts
			.iter()
			.map(tg::process::data::Mount::children)
			.into_iter()
			.flatten();
		std::iter::empty()
			.chain(logs)
			.chain(output)
			.chain(command)
			.chain(mounts)
			.collect()
	}
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	use serde::Deserialize as _;
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mount {
	pub source: Source,
	pub target: PathBuf,
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Source {
	Artifact(tg::artifact::Id),
	Path(PathBuf),
}

impl Mount {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match &self.source {
			Source::Artifact(artifact_id) => {
				let object_id: tg::object::Id = artifact_id.clone().into();
				std::iter::once(object_id).collect()
			},
			Source::Path(_) => BTreeSet::new(),
		}
	}
}

impl From<tg::command::data::Mount> for Mount {
	fn from(value: tg::command::data::Mount) -> Self {
		Self {
			source: Source::Artifact(value.source),
			target: value.target,
			readonly: true,
		}
	}
}
