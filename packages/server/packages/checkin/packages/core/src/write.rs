use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Option<Bytes>,
	pub children: Vec<Output>,
	pub data: Option<tg::blob::Data>,
	pub id: tg::blob::Id,
	pub length: u64,
	pub metadata: tg::object::Metadata,
	pub position: u64,
	pub put: [u8; 16],
}
