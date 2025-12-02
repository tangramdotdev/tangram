use {crate::Output, std::path::PathBuf, tangram_client as tg};

pub struct Abort;

#[expect(clippy::too_many_arguments)]
pub async fn run<H>(
	_handle: &H,
	_args: tg::value::data::Array,
	_cwd: PathBuf,
	_env: tg::value::data::Map,
	_executable: tg::command::data::Executable,
	_logger: crate::Logger,
	_main_runtime_handle: tokio::runtime::Handle,
	_abort_sender: Option<tokio::sync::watch::Sender<Option<Abort>>>,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	unimplemented!()
}

impl Abort {
	pub fn abort(&self) {}
}
