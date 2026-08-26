#[derive(Clone, Copy, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub partition_end: u64,
	pub partition_start: u64,
}
