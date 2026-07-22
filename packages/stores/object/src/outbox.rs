use bytes::Bytes;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Id(u128);

#[derive(Clone, Debug)]
pub struct Item {
	pub id: Id,
	pub partition: u64,
	pub payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub struct Key {
	pub id: Id,
	pub partition: u64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub keys: Vec<Key>,
}

#[derive(Clone, Copy, Debug)]
pub struct DequeueArg {
	pub batch_size: usize,
	pub partition_count: u64,
	pub partition_start: u64,
}

#[derive(Clone, Debug)]
pub struct EnqueueArg {
	pub partition: u64,
	pub payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub struct TryGetIdArg {
	pub id: Option<Id>,
	pub partition_count: u64,
	pub partition_start: u64,
}

impl Id {
	#[must_use]
	pub fn new(value: u128) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> u128 {
		self.0
	}
}
