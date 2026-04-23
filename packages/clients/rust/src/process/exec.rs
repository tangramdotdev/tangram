use {crate::prelude::*, std::os::unix::process::CommandExt as _};

pub async fn exec(arg: tg::process::Arg) -> tg::Result<()> {
	let handle = tg::handle()?;
	exec_with_handle(handle, arg).await
}

pub async fn exec_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<()>
where
	H: tg::Handle,
{
	tg::Process::<tg::Value>::exec_with_handle(handle, arg).await
}

impl<O> tg::Process<O> {
	pub async fn exec(arg: tg::process::Arg) -> tg::Result<()> {
		let handle = tg::handle()?;
		Self::exec_with_handle(handle, arg).await
	}

	pub async fn exec_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let arg = super::spawn::spawn_arg_with_handle(handle, arg).await?;
		if arg.sandbox.is_some() {
			return Err(tg::error!("an exec must not be sandboxed"));
		}
		validate_stdio(&arg.stdin, tg::process::stdio::Stream::Stdin)?;
		validate_stdio(&arg.stdout, tg::process::stdio::Stream::Stdout)?;
		validate_stdio(&arg.stderr, tg::process::stdio::Stream::Stderr)?;

		let prepared = tg::Process::<tg::Value>::prepare_unsandboxed_command(handle, &arg).await?;

		let mut command = std::process::Command::new(&prepared.executable);
		command.args(&prepared.args);
		command.env_clear();
		command.envs(&prepared.env);
		if let Some(cwd) = &prepared.cwd {
			command.current_dir(cwd);
		}
		command.stdin(convert_stdio(&arg.stdin));
		command.stdout(convert_stdio(&arg.stdout));
		command.stderr(convert_stdio(&arg.stderr));

		let source = command.exec();
		Err(tg::error!(
			!source,
			executable = %prepared.executable.display(),
			"failed to exec the process"
		))
	}
}

fn validate_stdio(
	stdio: &tg::process::Stdio,
	stream: tg::process::stdio::Stream,
) -> tg::Result<()> {
	if matches!(
		stdio,
		tg::process::Stdio::Inherit | tg::process::Stdio::Null
	) {
		Ok(())
	} else {
		Err(tg::error!(
			stream = %stream,
			"stdio must be inherit or null for an exec"
		))
	}
}

fn convert_stdio(stdio: &tg::process::Stdio) -> std::process::Stdio {
	match stdio {
		tg::process::Stdio::Inherit => std::process::Stdio::inherit(),
		tg::process::Stdio::Null => std::process::Stdio::null(),
		_ => unreachable!(),
	}
}
