use crate::{
	self as tg,
	util::serde::{is_false, is_true, return_true},
};
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub actual_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub cacheable: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<tg::Referent<tg::process::Id>>>,

	pub command: tg::command::Id,

	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub dequeued_at: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enqueued_at: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub expected_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub finished_at: Option<i64>,

	pub host: String,

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
	pub started_at: Option<i64>,

	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<tg::process::Stdio>,
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
			.flat_map(tg::process::data::Mount::children);
		std::iter::empty()
			.chain(logs)
			.chain(output)
			.chain(command)
			.chain(mounts)
			.collect()
	}
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mount {
	pub source: PathBuf,

	pub target: PathBuf,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

impl Mount {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		std::iter::empty()
	}
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	use serde::Deserialize as _;
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
