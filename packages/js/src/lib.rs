use {futures::future::BoxFuture, std::sync::Arc, tangram_client as tg};

#[cfg(feature = "quickjs")]
pub mod quickjs;
#[cfg(feature = "v8")]
pub mod v8;

pub enum Abort {
	#[cfg(feature = "quickjs")]
	QuickJs(self::quickjs::Abort),
	#[cfg(feature = "v8")]
	V8(self::v8::Abort),
}

impl Abort {
	pub fn abort(&self) {
		match self {
			#[cfg(feature = "quickjs")]
			Self::QuickJs(abort) => abort.abort(),
			#[cfg(feature = "v8")]
			Self::V8(abort) => abort.abort(),
			#[cfg(not(any(feature = "quickjs", feature = "v8")))]
			_ => {},
		}
	}
}

pub type Logger = Arc<
	dyn Fn(tg::process::log::Stream, Vec<u8>) -> BoxFuture<'static, tg::Result<()>>
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
