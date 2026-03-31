use {crate::prelude::*, std::io::IsTerminal as _};

pub async fn run<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	tg::Process::run(handle, arg).await
}

impl tg::Process {
	pub async fn run<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let sandbox = arg.sandbox.unwrap_or(false);

		let host = arg
			.host
			.ok_or_else(|| tg::error!("expected the host to be set"))?;

		let executable = arg
			.executable
			.ok_or_else(|| tg::error!("expected the executable to be set"))?;

		let mut builder = tg::Command::builder().host(host).executable(executable);

		builder = builder.args(arg.args);

		let cwd = if sandbox {
			None
		} else {
			let cwd = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
			Some(cwd)
		};
		let cwd = arg.cwd.or(cwd);
		builder = builder.cwd(cwd);

		let env = if sandbox {
			arg.env
		} else {
			let mut env = std::env::vars()
				.map(|(key, value)| (key, value.into()))
				.collect::<tg::value::Map>();
			env.remove("TANGRAM_OUTPUT");
			env
		};
		builder = builder.env(env);

		let process_mounts = arg
			.mounts
			.unwrap_or_default()
			.into_iter()
			.map(|mount| mount.to_data())
			.collect();

		builder = builder.stdin(None);
		builder = builder.user(arg.user);

		let command = builder.finish()?;
		let command_id = command.store(handle).await?;
		let mut command = tg::Referent::with_item(command_id);
		if let Some(name) = arg.name {
			command.options.name.replace(name);
		}

		let checksum = arg.checksum;

		let network = arg.network.unwrap_or_default();

		let stdin = arg.stdin;
		let stdout = arg.stdout;
		let stderr = arg.stderr;
		if network && checksum.is_none() {
			return Err(tg::error!(
				"a checksum is required to build with network enabled"
			));
		}

		let progress = arg.progress;

		let arg = tg::process::spawn::Arg {
			cached: arg.cached,
			checksum,
			command,
			local: None,
			mounts: process_mounts,
			network,
			parent: arg.parent,
			remotes: arg.remote.map(|remote| vec![remote]),
			retry: arg.retry,
			sandbox,
			stderr,
			stdin,
			stdout,
			tty: None,
		};
		let process = tg::Process::spawn_with_progress(handle, arg, |stream| {
			let writer = std::io::stderr();
			let is_tty = progress && writer.is_terminal();
			tg::progress::write_progress_stream(handle, stream, writer, is_tty)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		let output = process
			.output(handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process output"))?;

		Ok(output)
	}
}
