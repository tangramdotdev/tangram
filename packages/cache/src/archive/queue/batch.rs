#[derive(
	Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Id([u8; 16]);

impl Id {
	#[must_use]
	pub fn new(value: [u8; 16]) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> [u8; 16] {
		self.0
	}
}
