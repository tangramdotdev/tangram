use crate::Cli;
use tangram_client as tg;

pub mod check;
pub mod doc;
pub mod format;
pub mod get;
pub mod init;
pub mod new;
pub mod outdated;
pub mod publish;
pub mod search;
pub mod tree;
pub mod update;
pub mod yank;

/// Manage packages.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Check(self::check::Args),
	Doc(self::doc::Args),
	Format(self::format::Args),
	Get(self::get::Args),
	Init(self::init::Args),
	New(self::new::Args),
	Outdated(self::outdated::Args),
	Publish(self::publish::Args),
	Search(self::search::Args),
	Tree(self::tree::Args),
	Update(self::update::Args),
	Yank(self::yank::Args),
}

impl Cli {
	pub async fn command_package(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Check(args) => self.command_package_check(args).await,
			Command::Doc(args) => self.command_package_doc(args).await,
			Command::Format(args) => self.command_package_format(args).await,
			Command::Get(args) => self.command_package_get(args).await,
			Command::Init(args) => self.command_package_init(args).await,
			Command::New(args) => self.command_package_new(args).await,
			Command::Outdated(args) => self.command_package_outdated(args).await,
			Command::Publish(args) => self.command_package_publish(args).await,
			Command::Search(args) => self.command_package_search(args).await,
			Command::Tree(args) => self.command_package_tree(args).await,
			Command::Update(args) => self.command_package_update(args).await,
			Command::Yank(args) => self.command_package_yank(args).await,
		}
	}
}
