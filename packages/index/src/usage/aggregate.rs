#[derive(Clone, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub now: jiff::Timestamp,
	pub partition_end: u64,
	pub partition_start: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub count: usize,
}
