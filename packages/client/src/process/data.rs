use {
	crate as tg,
	std::path::PathBuf,
	tangram_util::serde::{is_false, is_true, return_true},
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub actual_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub cacheable: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<tg::Referent<tg::process::Id>>>,

	#[tangram_serialize(id = 3)]
	pub command: tg::command::Id,

	#[tangram_serialize(id = 4)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub dequeued_at: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "Option::is_none")]
	pub enqueued_at: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 9, default, skip_serializing_if = "Option::is_none")]
	pub expected_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 10, default, skip_serializing_if = "Option::is_none")]
	pub finished_at: Option<i64>,

	#[tangram_serialize(id = 11)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 12, default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 13, default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::process::data::Mount>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 14, default, skip_serializing_if = "is_false")]
	pub network: bool,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	#[tangram_serialize(id = 15, default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 16, default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 17, default, skip_serializing_if = "Option::is_none")]
	pub started_at: Option<i64>,

	#[tangram_serialize(id = 18)]
	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 19, default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 20, default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::process::Stdio>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 21, default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<tg::process::Stdio>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Mount {
	#[tangram_serialize(id = 0)]
	pub source: PathBuf,

	#[tangram_serialize(id = 1)]
	pub target: PathBuf,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	#[tangram_serialize(id = 2, default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	use serde::Deserialize as _;
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
