use {
	crate::Cli,
	std::path::PathBuf,
	tangram_client::prelude::*,
};

/// Run a command in a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub chroot: Option<PathBuf>,

	/// Change the working directory prior to spawn.
	#[arg(long, short = 'C')]
	pub cwd: Option<PathBuf>,

	/// Define environment variables.
	#[arg(
		action = clap::ArgAction::Append,
		num_args = 1,
		short = 'e',
		value_parser = super::parse_env,
	)]
	pub env: Vec<(String, String)>,

	/// The executable path.
	#[arg(index = 1)]
	pub executable: PathBuf,

	#[command(flatten)]
	pub options: super::Options,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_run(args: Args) -> std::process::ExitCode {
		// Create the command.
		let command = tangram_sandbox::Command {
			chroot: args.chroot,
			cwd: args.cwd,
			env: args.env,
			executable: args.executable,
			hostname: args.options.hostname,
			mounts: args
				.options
				.mounts
				.into_iter()
				.map(|mount| tangram_sandbox::Mount {
					source: mount.source,
					target: mount.target,
					fstype: mount.fstype,
					flags: mount.flags,
					data: mount.data,
				})
				.collect(),
			network: args.options.network.get(),
			trailing: args.trailing,
			user: args.options.user,
			stdin: None,
			stderr: None,
			stdout: None,
		};

		// Run the sandbox.
		#[cfg(target_os = "linux")]
		let result = tangram_sandbox::linux::spawn(command);

		#[cfg(target_os = "macos")]
		let result = tangram_sandbox::darwin::spawn(command);

		match result {
			Ok(status) => status,
			Err(error) => {
				let error = tg::error!(!error, "failed to run the sandbox");
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
