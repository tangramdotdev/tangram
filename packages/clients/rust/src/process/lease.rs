use crate::prelude::*;

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Lease([u8; 16]);

impl Lease {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(uuid::Uuid::now_v7().into_bytes())
	}

	#[must_use]
	pub fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}
}

impl std::fmt::Display for Lease {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::ENCODING.encode(&self.0))
	}
}

impl std::str::FromStr for Lease {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let bytes = tg::id::ENCODING
			.decode(s.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid lease"))?;
		let bytes = bytes
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("invalid lease"))?;
		let lease = Self(bytes);
		Ok(lease)
	}
}
