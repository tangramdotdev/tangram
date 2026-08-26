use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Arg {
	pub fragments: Vec<Bytes>,
	pub id: super::Id,
	pub partition: u64,
}
