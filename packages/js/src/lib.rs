use tangram_client::prelude::*;

mod host;
mod stdio;

#[cfg(feature = "quickjs")]
pub mod quickjs;
#[cfg(feature = "v8")]
pub mod v8;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}
