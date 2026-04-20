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
		let sandbox = super::normalize_sandbox_arg(arg.sandbox.clone(), arg.cpu, arg.memory)?;
		let sandboxed = sandbox.is_some();

		let host = arg
			.host
			.ok_or_else(|| tg::error!("expected the host to be set"))?;

		let executable = arg
			.executable
			.ok_or_else(|| tg::error!("expected the executable to be set"))?;

		let mut builder = tg::Command::builder().host(host).executable(executable);

		builder = builder.args(arg.args);

		let cwd = if sandboxed {
			None
		} else {
			let cwd = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
			Some(cwd)
		};
		let cwd = arg.cwd.or(cwd);
		builder = builder.cwd(cwd);

		let env = if sandboxed {
			arg.env
		} else {
			let mut env = tg::process::env()?;
			env.remove("TANGRAM_OUTPUT");
			env.remove("TANGRAM_PROCESS");
			env
		};
		builder = builder.env(env);

		builder = builder.stdin(None);
		builder = builder.user(arg.user);

		let command = builder.finish()?;
		let command_id = command.store_with_handle(handle).await?;
		let mut command = tg::Referent::with_item(command_id);
		if let Some(name) = arg.name {
			command.options.name.replace(name);
		}

		let checksum = arg.checksum;

		let stdin = arg.stdin;
		let stdout = arg.stdout;
		let stderr = arg.stderr;
		let sandbox_arg = match sandbox.clone() {
			Some(tg::Either::Left(arg)) => Some(arg),
			Some(tg::Either::Right(_)) | None => None,
		};
		if sandbox_arg.as_ref().is_some_and(|arg| arg.network) && checksum.is_none() {
			return Err(tg::error!(
				"a checksum is required to build with network enabled"
			));
		}

		let progress = arg.progress;

		let arg = tg::process::spawn::Arg {
			cached: arg.cached,
			cache_location: None,
			checksum,
			command,
			location: arg.location,
			parent: arg.parent,
			retry: arg.retry,
			sandbox,
			stderr,
			stdin,
			stdout,
			tty: arg.tty,
		};
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
