#[derive(Clone, Copy, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub now: i64,
	pub partition: u64,
}
