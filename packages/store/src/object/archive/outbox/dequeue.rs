#[derive(Clone, Copy, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub bound: Option<[u8; 16]>,
	pub cursor: Option<[u8; 16]>,
	pub partition_end: u64,
	pub partition_start: u64,
}
