use crate::Cli;
use tangram_client as tg;

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

/// Manage tags.
#[derive(Clone, Debug, clap::Args)]
#[command(
	args_conflicts_with_subcommands = true,
	subcommand_negates_reqs = true,
	subcommand_precedence_over_arg = true
)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub args: crate::tag::put::Args,

	#[command(subcommand)]
	pub command: Option<Command>,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Delete(self::delete::Args),
	Get(self::get::Args),
	List(self::list::Args),
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_tag(&mut self, args: Args) -> tg::Result<()> {
		match args.command.unwrap_or(Command::Put(args.args)) {
			Command::Delete(args) => {
				self.command_tag_delete(args).await?;
			},
			Command::Get(args) => {
				self.command_tag_get(args).await?;
			},
			Command::List(args) => {
				self.command_tag_list(args).await?;
			},
			Command::Put(args) => {
				self.command_tag_put(args).await?;
			},
		}
		Ok(())
	}
}
