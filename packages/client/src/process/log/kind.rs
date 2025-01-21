use crate as tg;

#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
#[repr(i32)]
pub enum Kind {
	Stdout = 1,
	Stderr = 2,
}

impl Default for Kind {
	fn default() -> Self {
		Self::Stdout
	}
}

impl TryFrom<i32> for Kind {
	type Error = tg::Error;
	fn try_from(value: i32) -> Result<Self, Self::Error> {
		match value {
			1 => Ok(Self::Stdout),
			2 => Ok(Self::Stderr),
			_ => Err(tg::error!(%value, "invalid log chunk kind")),
		}
	}
}

impl Into<i32> for Kind {
	fn into(self) -> i32 {
		self as i32
	}
}

impl serde_with::SerializeAs<Kind> for i32 {
	fn serialize_as<S>(source: &Kind, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_i32(*source as i32)
	}
}

impl<'de> serde_with::DeserializeAs<'de, Kind> for i32 {
	fn deserialize_as<D>(deserializer: D) -> Result<Kind, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;
		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = Kind;
			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(formatter, "expected stdout or stderr")
			}

			fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				match v {
					1 => Ok(Kind::Stdout),
					2 => Ok(Kind::Stderr),
					_ => Err(E::custom("expected stdout or stderr")),
				}
			}
		}
		deserializer.deserialize_i32(Visitor)
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}
