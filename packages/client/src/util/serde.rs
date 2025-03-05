use std::borrow::Cow;

use crate as tg;
use bytes::Bytes;

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

		impl serde::de::Visitor<'_> for Visitor {
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

pub struct CommaSeparatedString;

impl<T> serde_with::SerializeAs<Vec<T>> for CommaSeparatedString
where
	T: std::fmt::Display,
{
	fn serialize_as<S>(source: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.collect_str(&CommaSeparatedStringDisplay(source))
	}
}

impl<'de, T> serde_with::DeserializeAs<'de, Vec<T>> for CommaSeparatedString
where
	T: std::str::FromStr,
	T::Err: std::fmt::Display,
{
	fn deserialize_as<D>(deserializer: D) -> Result<Vec<T>, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let s: Cow<'_, str> = serde::Deserialize::deserialize(deserializer)?;
		if s.is_empty() {
			return Ok(Vec::new());
		}
		s.split(',')
			.map(|item| T::from_str(item).map_err(serde::de::Error::custom))
			.collect()
	}
}

struct CommaSeparatedStringDisplay<'a, T>(&'a Vec<T>);

impl<T> std::fmt::Display for CommaSeparatedStringDisplay<'_, T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut first = true;
		for item in self.0 {
			if first {
				first = false;
			} else {
				write!(f, ",")?;
			}
			write!(f, "{item}")?;
		}
		Ok(())
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

		impl serde::de::Visitor<'_> for Visitor {
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
pub fn return_false() -> bool {
	false
}

#[must_use]
pub fn return_true() -> bool {
	true
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn is_false(value: &bool) -> bool {
	!*value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn is_true(value: &bool) -> bool {
	*value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn is_zero(value: &usize) -> bool {
	*value == 0
}
