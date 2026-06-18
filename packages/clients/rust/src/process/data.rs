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
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub actual_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub cacheable: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<tg::process::data::Child>>,

	#[tangram_serialize(id = 3)]
	pub command: tg::command::Id,

	#[tangram_serialize(id = 4)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 20, skip_serializing_if = "Option::is_none")]
	pub debug: Option<tg::process::Debug>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::MaybeWithToken<tg::error::Id>>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Option::is_none")]
	pub expected_checksum: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Option::is_none")]
	pub finished_at: Option<i64>,

	#[tangram_serialize(id = 9)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::MaybeWithToken<tg::blob::Id>>,

	#[tangram_serialize(id = 11)]
	pub sandbox: tg::sandbox::Id,

	#[serde(
		default,
		deserialize_with = "deserialize_output",
		skip_serializing_if = "Option::is_none"
	)]
	#[tangram_serialize(default, id = 12, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::value::Data>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 13, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 14, skip_serializing_if = "Option::is_none")]
	pub started_at: Option<i64>,

	#[tangram_serialize(id = 15)]
	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 16, skip_serializing_if = "is_default")]
	pub stderr: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 17, skip_serializing_if = "is_default")]
	pub stdin: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 18, skip_serializing_if = "is_default")]
	pub stdout: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 19, skip_serializing_if = "Option::is_none")]
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
	pub process: tg::MaybeWithToken<tg::process::Id>,

	#[tangram_serialize(id = 3)]
	pub options: tg::referent::Options,
}

impl Data {
	pub fn remove_tokens(&mut self) {
		if let Some(children) = &mut self.children {
			for child in children {
				child.remove_tokens();
			}
		}
		if let Some(tg::Either::Right(error)) = &mut self.error
			&& let tg::Either::Right(error_with_token) = error
		{
			*error = tg::Either::Left(error_with_token.id.clone());
		}
		if let Some(tg::Either::Right(log)) = &self.log {
			self.log = Some(tg::Either::Left(log.id.clone()));
		}
		if let Some(output) = &mut self.output {
			output.remove_tokens();
		}
	}
}

impl Child {
	pub fn remove_tokens(&mut self) {
		if let tg::Either::Right(process) = &self.process {
			self.process = tg::Either::Left(process.id.clone());
		}
	}
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
