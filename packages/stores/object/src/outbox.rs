use bytes::Bytes;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchId([u8; 16]);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FragmentIndex(u64);

#[derive(Clone, Debug)]
pub struct Batch {
	pub fragments: Vec<Bytes>,
	pub id: BatchId,
	pub partition: u64,
}

#[derive(Clone, Debug)]
pub struct Fragment {
	pub batch: BatchId,
	pub index: FragmentIndex,
	pub partition: u64,
	pub payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub struct FragmentKey {
	pub batch: BatchId,
	pub index: FragmentIndex,
	pub partition: u64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub fragments: Vec<FragmentKey>,
}

#[derive(Clone, Copy, Debug)]
pub struct DequeueArg {
	pub batch_size: usize,
	pub partition_end: u64,
	pub partition_start: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct TryGetBatchArg {
	pub batch: Option<BatchId>,
	pub partition_end: u64,
	pub partition_start: u64,
}

impl BatchId {
	#[must_use]
	pub fn new(value: [u8; 16]) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> [u8; 16] {
		self.0
	}
}

impl FragmentIndex {
	#[must_use]
	pub fn new(value: u64) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> u64 {
		self.0
	}
}
