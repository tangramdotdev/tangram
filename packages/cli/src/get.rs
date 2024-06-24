use crate::Cli;
use either::Either;
use tangram_client as tg;

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let item = args.reference.get(&client).await?;
		match item {
			Either::Left(build) => {
				self.command_build_get(crate::build::get::Args { build })
					.await?;
			},
			Either::Right(object) => {
				self.command_object_get(crate::object::get::Args { object })
					.await?;
			},
		}
		Ok(())
	}
}
