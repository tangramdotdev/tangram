use {futures::future::BoxFuture, std::sync::Arc, tangram_client as tg};

#[cfg(feature = "boa")]
pub mod boa;
#[cfg(feature = "v8")]
pub mod v8;

#[cfg(all(feature = "boa", not(feature = "v8")))]
pub use self::boa::{Abort, run};
#[cfg(feature = "v8")]
pub use self::v8::{Abort, run};

pub type Logger = Arc<
	dyn Fn(tg::process::log::Stream, String) -> BoxFuture<'static, tg::Result<()>>
		+ Send
		+ Sync
		+ 'static,
>;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}
