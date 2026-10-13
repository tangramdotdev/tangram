use {crate::Cli, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
mod linux;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, default_value = "builtin")]
	pub backend: tangram_sandbox::Kind,
	#[cfg(target_os = "linux")]
	#[command(flatten)]
	pub linux: self::linux::Args,
}

impl Cli {
	pub fn command_sandbox_init(args: Args) -> tg::Result<std::process::ExitCode> {
		match args.backend {
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Builtin
			| tangram_sandbox::Kind::Bubblewrap
			| tangram_sandbox::Kind::Gvisor => Self::command_sandbox_linux_init(args.linux),
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::CloudHypervisor => tangram_sandbox::cloud_hypervisor::init::run(),
			#[cfg(target_os = "macos")]
			tangram_sandbox::Kind::Seatbelt => Err(tg::error!(
				"the seatbelt backend starts the sandbox server directly"
			)),
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Seatbelt => Err(tg::error!("the seatbelt backend requires macos")),
			#[cfg(target_os = "macos")]
			_ => Err(tg::error!("the selected backend requires linux")),
		}
	}
}
