use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Inherited raw fd of the control socket. The server populates this with a
	/// non-`FD_CLOEXEC` end of a Unix socketpair when spawning the helper.
	#[arg(long)]
	pub control_fd: i32,
}

impl Cli {
	pub fn command_sandbox_helper(args: &Args) -> tg::Result<std::process::ExitCode> {
		tangram_sandbox::rootless::helper::helper_main(args.control_fd)
	}
}
