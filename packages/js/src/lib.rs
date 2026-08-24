use {std::path::PathBuf, tangram_client::prelude::*};

mod host;
mod http2;

pub mod inspect;
#[cfg(feature = "quickjs")]
pub mod quickjs;
pub mod repl;
#[cfg(feature = "v8")]
pub mod v8;

pub struct Arg {
	pub args: tg::value::data::Array,
	pub cwd: PathBuf,
	pub env: tg::value::data::Map,
	pub export: Option<String>,
	pub handle: tg::handle::dynamic::Handle,
	pub host: Option<String>,
	pub http: tg::Http,
	pub inspect: Option<self::inspect::Options>,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub module: tg::module::Data,
	pub repl: Option<self::repl::Receiver>,
}

#[derive(serde::Serialize)]
pub struct MagicOutput {
	pub export: Option<String>,
	pub module: tg::module::Data,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}
