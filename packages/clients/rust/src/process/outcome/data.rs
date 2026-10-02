use {crate::prelude::*, serde::Deserialize as _, serde_with::serde_as};

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
	#[serde_as(as = "Option<Error>")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,

	#[tangram_serialize(id = 1)]
	pub exit: u8,

	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::unwrap_or_skip"
	)]
	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub output: Option<tg::value::Data>,
}

struct Error;

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
