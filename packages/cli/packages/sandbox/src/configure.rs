use {tangram_client::prelude::*, usage_rs as usage};

#[derive(Clone, Copy, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long, requires("--user-namespace-fd"))]
	pub network_namespace_fd: Option<i32>,

	#[usage(long, requires("--network-namespace-fd"))]
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
