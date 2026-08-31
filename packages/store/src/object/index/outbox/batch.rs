#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id([u8; 16]);

pub mod delete;
pub mod enqueue;
pub mod get;

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
