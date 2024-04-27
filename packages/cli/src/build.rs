use crate::Cli;
use tangram_client as tg;

pub mod create;
pub mod get;
pub mod pull;
pub mod push;
pub mod put;
pub mod tree;

/// Build a target or manage builds.
#[derive(Debug, clap::Args)]
#[clap(args_conflicts_with_subcommands = true)]
pub struct Args {
	#[clap(flatten)]
	pub args: self::create::Args,
	#[clap(subcommand)]
	pub command: Option<Command>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, clap::Subcommand)]
pub enum Command {
	#[clap(hide = true)]
	Create(self::create::Args),
	Get(self::get::Args),
	Put(self::put::Args),
	Push(self::push::Args),
	Pull(self::pull::Args),
	Tree(self::tree::Args),
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> tg::Result<()> {
		match args.command.unwrap_or(Command::Create(args.args)) {
			Command::Create(args) => {
				self.command_build_create(args).await?;
			},
			Command::Get(args) => {
				self.command_build_get(args).await?;
			},
			Command::Put(args) => {
				self.command_build_put(args).await?;
			},
			Command::Push(args) => {
				self.command_build_push(args).await?;
			},
			Command::Pull(args) => {
				self.command_build_pull(args).await?;
			},
			Command::Tree(args) => {
				self.command_build_tree(args).await?;
			},
		}
		Ok(())
	}
}
