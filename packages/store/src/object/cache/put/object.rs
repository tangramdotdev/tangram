#[derive(Clone, Debug)]
pub struct Arg {
	pub cached_at: i64,
	pub object: crate::object::put::Arg,
	pub partition: u64,
}
