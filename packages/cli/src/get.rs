use crate::Cli;
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
		let item = args.reference.get(&handle).await?;
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
