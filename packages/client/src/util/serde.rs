use crate as tg;
use bytes::Bytes;
use either::Either;
use serde::Deserialize;
use std::marker::PhantomData;

pub struct BytesBase64;

impl serde_with::SerializeAs<Bytes> for BytesBase64 {
	fn serialize_as<S>(value: &Bytes, serializer: S) -> tg::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let string = data_encoding::BASE64.encode(value);
		serializer.serialize_str(&string)
	}
}

impl<'de> serde_with::DeserializeAs<'de, Bytes> for BytesBase64 {
	fn deserialize_as<D>(deserializer: D) -> tg::Result<Bytes, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;

		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = Bytes;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a base64 encoded string")
			}

			fn visit_str<E>(self, value: &str) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				let bytes = data_encoding::BASE64
					.decode(value.as_bytes())
					.map_err(|_| serde::de::Error::custom("invalid string"))?
					.into();
				Ok(bytes)
			}
		}

		deserializer.deserialize_any(Visitor)
	}
}

pub struct EitherUntagged<L, R>(PhantomData<(L, R)>);

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum Either_<L, R> {
	Left(L),
	Right(R),
}

impl<L, R, LAs, RAs> serde_with::SerializeAs<Either<L, R>> for EitherUntagged<LAs, RAs>
where
	L: serde::Serialize,
	R: serde::Serialize,
	LAs: serde_with::SerializeAs<L>,
	RAs: serde_with::SerializeAs<R>,
{
	fn serialize_as<S>(value: &Either<L, R>, serializer: S) -> tg::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match value {
			Either::Left(value) => value.serialize(serializer),
			Either::Right(value) => value.serialize(serializer),
		}
	}
}

impl<'de, L, R, LAs, RAs> serde_with::DeserializeAs<'de, Either<L, R>> for EitherUntagged<LAs, RAs>
where
	L: serde::Deserialize<'de>,
	R: serde::Deserialize<'de>,
	LAs: serde_with::DeserializeAs<'de, L>,
	RAs: serde_with::DeserializeAs<'de, R>,
{
	fn deserialize_as<D>(deserializer: D) -> tg::Result<Either<L, R>, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let either = Either_::deserialize(deserializer)?;
		let either = match either {
			Either_::Left(left) => Either::Left(left),
			Either_::Right(right) => Either::Right(right),
		};
		Ok(either)
	}
}

pub struct SeekFromString;

impl serde_with::SerializeAs<std::io::SeekFrom> for SeekFromString {
	fn serialize_as<S>(value: &std::io::SeekFrom, serializer: S) -> tg::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let string = match value {
			std::io::SeekFrom::Start(seek) => format!("start.{seek}"),
			std::io::SeekFrom::End(seek) => format!("end.{seek}"),
			std::io::SeekFrom::Current(seek) => format!("current.{seek}"),
		};
		serializer.serialize_str(&string)
	}
}

impl<'de> serde_with::DeserializeAs<'de, std::io::SeekFrom> for SeekFromString {
	fn deserialize_as<D>(deserializer: D) -> tg::Result<std::io::SeekFrom, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;

		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = std::io::SeekFrom;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a string")
			}

			fn visit_str<E>(self, value: &str) -> tg::Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				if let Some(seek) = value.strip_prefix("start.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Start(seek))
				} else if let Some(seek) = value.strip_prefix("end.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::End(seek))
				} else if let Some(seek) = value.strip_prefix("current.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Current(seek))
				} else {
					let seek = value
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Start(seek))
				}
			}
		}

		deserializer.deserialize_str(Visitor)
	}
}

#[must_use]
pub fn return_true() -> bool {
	true
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn is_true(value: &bool) -> bool {
	*value
}
