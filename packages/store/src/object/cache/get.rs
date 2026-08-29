#[derive(Clone, Copy, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub partition: u64,
}
