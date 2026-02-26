use {
	crate::Cli, futures::prelude::*, std::path::PathBuf, tangram_client::prelude::*,
	tangram_futures::task::Task,
};

/// Execute a command in a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
#[allow(clippy::struct_field_names)]
pub struct Args {
	/// The sandbox ID.
	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,

	/// The command to execute.
	#[arg(index = 2)]
	pub command: PathBuf,

	/// The arguments to the command.
	#[arg(index = 3, trailing_var_arg = true)]
	pub args: Vec<String>,

	#[command(flatten)]
	pub tty: Tty,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Tty {
	/// Whether to allocate a terminal.
	#[arg(
		default_missing_value = "true",
		short,
		long,
		num_args = 0..=1,
		overrides_with = "no_tty",
		require_equals = true,
	)]
	tty: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "tty",
		require_equals = true,
	)]
	no_tty: Option<bool>,
}

impl Tty {
	pub fn get(&self) -> bool {
		self.tty.or(self.no_tty.map(|v| !v)).unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_sandbox_exec(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let client = handle
			.left()
			.ok_or_else(|| tg::error!("this command requires a client, not a server"))?;

		// Create the stdio.
		let stdio = crate::run::stdio::Stdio::new(&client, None, args.tty.get())
			.await
			.map_err(|source| tg::error!(!source, "failed to create stdio"))?;

		// Spawn the command in the sandbox.
		let arg = tg::sandbox::spawn::Arg {
			command: args.command,
			args: args.args,
			env: std::collections::BTreeMap::new(),
			stdin: stdio.stdin.clone().unwrap(),
			stdout: stdio.stdout.clone().unwrap(),
			stderr: stdio.stderr.clone().unwrap(),
		};
		let output = client
			.sandbox_spawn(&args.sandbox, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the command in the sandbox"))?;

		// Enable raw mode if necessary.
		if let Some(tty) = &stdio.tty {
			tty.enable_raw_mode()?;
		}

		// Spawn the stdio task.
		let stdio_task = Task::spawn({
			let handle = client.clone();
			let stdio = stdio.clone();
			|stop| async move { crate::run::stdio::task(&handle, stop, stdio).boxed().await }
		});

		// Wait for the process to finish.
		let arg = tg::sandbox::wait::Arg { pid: output.pid };
		let result = client.sandbox_wait(&args.sandbox, arg).await;

		// Cleanup stdio.
		stdio.close(&client).await?;
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;
		stdio.delete(&client).await?;

		// Handle the result.
		let wait = result.map_err(|source| {
			tg::error!(!source, "failed to wait for the command in the sandbox")
		})?;

		// Set the exit code.
		if wait.status != 0 {
			let exit = u8::try_from(wait.status).unwrap_or(1);
			self.exit.replace(exit);
		}

		Ok(())
	}
}
