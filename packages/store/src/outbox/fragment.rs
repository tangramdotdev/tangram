use bytes::Bytes;

pub mod delete;
pub mod dequeue;

#[derive(Clone, Debug)]
pub struct Fragment {
	pub batch: super::batch::Id,
	pub index: Index,
	pub partition: u64,
	pub payload: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Index(u64);

#[derive(Clone, Copy, Debug)]
pub struct Key {
	pub batch: super::batch::Id,
	pub index: Index,
	pub partition: u64,
}

impl Index {
	#[must_use]
	pub fn new(value: u64) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> u64 {
		self.0
	}
}
