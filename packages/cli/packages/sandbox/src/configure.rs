use tangram_client::prelude::*;

#[derive(Clone, Copy, Debug, clap::Args)]
pub struct Args {
	#[arg(long, requires = "user_namespace_fd")]
	pub network_namespace_fd: Option<i32>,

	#[arg(long, requires = "network_namespace_fd")]
	pub user_namespace_fd: Option<i32>,
}

pub fn run(args: Args) -> tg::Result<std::process::ExitCode> {
	match (args.network_namespace_fd, args.user_namespace_fd) {
		(Some(network), Some(user)) => tangram_sandbox::configure::hold(network, user),
		(None, None) => tangram_sandbox::configure::run(),
		_ => Err(tg::error!(
			"the namespace descriptors must be provided together"
		)),
	}
}
