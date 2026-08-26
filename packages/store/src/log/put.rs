use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub bytes: Bytes,
	pub position: u64,
	pub process: tg::process::Id,
	pub stream: tg::process::stdio::Stream,
	pub stream_position: u64,
	pub timestamp: i64,
}
