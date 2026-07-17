use {
	crate::prelude::*,
	serde::Deserialize as _,
	serde_with::{DisplayFromStr, serde_as},
	tangram_util::serde::{is_default, is_false},
};

#[serde_as]
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
	#[serde_as(as = "Option<Error>")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,

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
	#[serde_as(as = "Option<DisplayFromStr>")]
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::Referent<tg::blob::Id>>,

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

#[serde_as]
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
	#[serde_as(as = "DisplayFromStr")]
	pub process: tg::Referent<tg::process::Id>,
}

struct Error;

impl Data {
	#[must_use]
	pub fn without_tokens(mut self) -> Self {
		self.children = self
			.children
			.map(|children| children.into_iter().map(Child::without_tokens).collect());
		self.error = self.error.map(|error| match error {
			tg::Either::Left(error) => tg::Either::Left(error.without_tokens()),
			tg::Either::Right(mut error) => {
				error.options.token.take();
				tg::Either::Right(error)
			},
		});
		self.log = self.log.map(|mut log| {
			log.options.token.take();
			log
		});
		self.output = self.output.map(tg::value::Data::without_tokens);

		self
	}
}

impl Child {
	#[must_use]
	pub fn without_tokens(mut self) -> Self {
		self.process.options.token.take();

		self
	}
}

impl serde_with::SerializeAs<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>> for Error {
	fn serialize_as<S>(
		source: &tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
		serializer: S,
	) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match source {
			tg::Either::Left(data) => serde::Serialize::serialize(data, serializer),
			tg::Either::Right(referent) => serializer.collect_str(referent),
		}
	}
}

impl<'de> serde_with::DeserializeAs<'de, tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>
	for Error
{
	fn deserialize_as<D>(
		deserializer: D,
	) -> Result<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let value = tg::Either::<tg::error::Data, String>::deserialize(deserializer)?;
		let value = match value {
			tg::Either::Left(data) => tg::Either::Left(data),
			tg::Either::Right(value) => {
				let referent = value.parse().map_err(serde::de::Error::custom)?;
				tg::Either::Right(referent)
			},
		};

		Ok(value)
	}
}

fn deserialize_output<'de, D>(deserializer: D) -> Result<Option<tg::value::Data>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	Ok(Option::deserialize(deserializer)?.or(Some(tg::value::Data::Null)))
}
