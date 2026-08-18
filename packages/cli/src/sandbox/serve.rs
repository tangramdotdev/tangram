use {
	crate::Cli,
	std::{path::PathBuf, time::Duration},
	tangram_client::prelude::*,
};

/// Serve sandbox requests.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, value_parser = humantime::parse_duration)]
	pub control_tcp_keep_alive_interval: Duration,

	#[arg(long, value_parser = humantime::parse_duration)]
	pub control_tcp_keep_alive_timeout: Duration,

	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[command(flatten)]
	pub listen: Listen,

	#[arg(long)]
	pub output_path: PathBuf,

	#[arg(long)]
	pub url: tangram_uri::Uri,

	#[arg(long)]
	pub tangram_path: PathBuf,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Listen {
	#[arg(
		default_missing_value = "true",
		id = "serve.listen.connect",
		long = "connect",
		num_args = 0..=1,
		overrides_with = "serve.listen.listen",
		require_equals = true,
	)]
	connect: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "serve.listen.listen",
		long = "listen",
		num_args = 0..=1,
		overrides_with = "serve.listen.connect",
		require_equals = true,
	)]
	listen: Option<bool>,
}

impl Listen {
	#[must_use]
	pub fn get(&self) -> bool {
		self.listen
			.or(self.connect.map(|value| !value))
			.unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_sandbox_serve(&mut self, args: Args) -> tg::Result<()> {
		let arg = tangram_sandbox::serve::Arg {
			control_tcp_keep_alive: tangram_sandbox::KeepAlive {
				interval: args.control_tcp_keep_alive_interval,
				timeout: args.control_tcp_keep_alive_timeout,
			},
			library_paths: args.library_paths,
			listen: args.listen.get(),
			output_path: args.output_path,
			tangram_path: args.tangram_path,
			url: args.url,
		};
		tangram_sandbox::serve::run(&arg).await?;
		Ok(())
	}
}
