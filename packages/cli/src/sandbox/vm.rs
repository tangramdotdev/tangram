use {crate::Cli, tangram_client::prelude::*};

pub mod create_rootfs;
pub mod init;
pub mod run;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(name = "create-rootfs")]
	CreateRootfs(self::create_rootfs::Args),
	#[command(hide = true)]
	Init(self::init::Args),
	#[command(hide = true)]
	Run(self::run::Args),
}

impl Cli {
	pub async fn command_sandbox_vm(&mut self, args: Args) -> tg::Result<()> {
		let exit = match args.command {
			Command::CreateRootfs(args) => Self::command_sandbox_vm_create_rootfs(args)?,
			Command::Init(args) => Self::command_sandbox_vm_init(args)?,
			Command::Run(args) => Self::command_sandbox_vm_run(args)?,
		};
		self.exit.replace(exit);
		Ok(())
	}
}
