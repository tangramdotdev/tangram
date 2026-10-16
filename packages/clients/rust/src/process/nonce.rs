use crate::prelude::*;

/// A nonce that names a connect, so that other requests can locate its process before they know its ID. A nonce is 16 bytes, written with the ID encoding.
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
pub struct Nonce([u8; 16]);

impl Nonce {
	/// Generate a new nonce.
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

impl std::fmt::Display for Nonce {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::ENCODING.encode(&self.0))
	}
}

impl std::str::FromStr for Nonce {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let bytes = tg::id::ENCODING
			.decode(value.as_bytes())
			.ok()
			.and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
			.ok_or_else(|| tg::error!(%value, "invalid nonce"))?;
		let nonce = Self(bytes);
		Ok(nonce)
	}
}
