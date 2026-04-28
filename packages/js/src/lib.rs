use {std::path::PathBuf, tangram_client::prelude::*};

mod host;
mod stdio;

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
	pub executable: tg::command::data::Executable,
	pub handle: tg::handle::dynamic::Handle,
	pub host: Option<String>,
	pub inspect: Option<self::inspect::Options>,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub repl: Option<self::repl::Receiver>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}
