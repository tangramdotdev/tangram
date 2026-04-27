use {
	crate::prelude::*,
	serde::Deserialize as _,
	tangram_util::serde::{is_default, is_false},
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
	pub children: Option<Vec<tg::process::data::Child>>,

	#[tangram_serialize(id = 3)]
	pub command: tg::command::Id,

	#[tangram_serialize(id = 4)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 20, default, skip_serializing_if = "Option::is_none")]
	pub debug: Option<tg::process::Debug>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Option::is_none")]
	pub expected_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "Option::is_none")]
	pub finished_at: Option<i64>,

	#[tangram_serialize(id = 9)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 10, default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,

	#[tangram_serialize(id = 11)]
	pub sandbox: tg::sandbox::Id,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	#[tangram_serialize(id = 12, default, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 13, default, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 14, default, skip_serializing_if = "Option::is_none")]
	pub started_at: Option<i64>,

	#[tangram_serialize(id = 15)]
	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 16, default, skip_serializing_if = "is_default")]
	pub stderr: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 17, default, skip_serializing_if = "is_default")]
	pub stdin: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 18, default, skip_serializing_if = "is_default")]
	pub stdout: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 19, default, skip_serializing_if = "Option::is_none")]
	pub tty: Option<tg::process::Tty>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Child {
	#[tangram_serialize(id = 1)]
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[tangram_serialize(id = 2)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 3)]
	pub options: tg::referent::Options,
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
