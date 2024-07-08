use crate::Cli;
use tangram_client as tg;

pub mod check;
pub mod checkin;
pub mod doc;
pub mod format;
pub mod init;
pub mod new;
pub mod outdated;
pub mod update;

/// Manage packages.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Check(self::check::Args),
	Checkin(self::checkin::Args),
	Doc(self::doc::Args),
	Format(self::format::Args),
	Init(self::init::Args),
	New(self::new::Args),
	Outdated(self::outdated::Args),
	Update(self::update::Args),
}

impl Cli {
	pub async fn command_package(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Check(args) => self.command_package_check(args).await,
			Command::Checkin(args) => self.command_package_checkin(args).await,
			Command::Doc(args) => self.command_package_doc(args).await,
			Command::Format(args) => self.command_package_format(args).await,
			Command::Init(args) => self.command_package_init(args).await,
			Command::New(args) => self.command_package_new(args).await,
			Command::Outdated(args) => self.command_package_outdated(args).await,
			Command::Update(args) => self.command_package_update(args).await,
		}
	}
}
