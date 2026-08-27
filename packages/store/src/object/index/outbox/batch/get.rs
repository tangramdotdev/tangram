#[derive(Clone, Copy, Debug)]
pub struct Arg {
	pub batch: Option<super::Id>,
	pub partition_end: u64,
	pub partition_start: u64,
}
