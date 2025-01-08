use std::path::Path;

use crate::Cli;
use tangram_client as tg;

pub mod check;
pub mod document;
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
	#[command(alias = "doc")]
	Document(self::document::Args),
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
			Command::Document(args) => self.command_package_document(args).await,
			Command::Format(args) => self.command_package_format(args).await,
			Command::Init(args) => self.command_package_init(args).await,
			Command::New(args) => self.command_package_new(args).await,
			Command::Outdated(args) => self.command_package_outdated(args).await,
			Command::Update(args) => self.command_package_update(args).await,
		}
	}
}


fn infer_module_kind(path: impl AsRef<Path>) -> Option<tg::module::Kind> {
	let path = path.as_ref();
	if path.ends_with(".d.ts") {
		Some(tg::module::Kind::Dts)
	} else if path.extension().map_or(false, |extension| extension.eq_ignore_ascii_case("js")) {
		Some(tg::module::Kind::Js)
	} else if path.extension().map_or(false, |extension| extension.eq_ignore_ascii_case("ts")) {
		Some(tg::module::Kind::Ts)
	} else {
		None
	}
}