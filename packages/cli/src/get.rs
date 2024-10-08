use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client as tg;
use tangram_either::Either;

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let item = self.get_reference(&args.reference).await?;
		let id = match &item {
			Either::Left(build) => Either::Left(build.id()),
			Either::Right(object) => Either::Right(object.id(&handle).await?),
		};
		eprintln!("{} {id}", "info".blue().bold());
		match item {
			Either::Left(build) => {
				let build = build.id().clone();
				self.command_build_get(crate::build::get::Args { build })
					.await?;
			},
			Either::Right(object) => {
				let object = object.id(&handle).await?.clone();
				self.command_object_get(crate::object::get::Args { object })
					.await?;
			},
		}
		Ok(())
	}
}
