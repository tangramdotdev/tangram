use crate::prelude::*;

pub async fn run(arg: tg::process::Arg) -> tg::Result<tg::Value> {
	let handle = tg::handle()?;
	run_with_handle(handle, arg).await
}

pub async fn run_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	tg::Process::<tg::Value>::run_with_handle(handle, arg).await
}

impl<O> tg::Process<O> {
	pub async fn run(arg: tg::process::Arg) -> tg::Result<O>
	where
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let handle = tg::handle()?;
		Self::run_with_handle(handle, arg).await
	}

	pub async fn run_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<O>
	where
		H: tg::Handle,
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let progress = arg.progress;
		let process = tg::Process::<O>::spawn_with_progress_with_handle(handle, arg, |stream| {
			let writer = std::io::stderr();
			let is_tty =
				progress && tangram_util::tty::is_foreground_controlling_tty(libc::STDERR_FILENO);
			tg::progress::write_progress_stream(handle, stream, writer, is_tty)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		let output = process
			.output_with_handle(handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process output"))?;

		Ok(output)
	}
}
