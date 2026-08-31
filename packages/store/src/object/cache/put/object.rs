#[derive(Clone, Debug)]
pub struct Arg {
	pub cache: [u8; 16],
	pub object: crate::object::put::Arg,
	pub partition: u64,
}
