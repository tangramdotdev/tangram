use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The object or process.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_children(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			Either::Left(process) => {
				self.command_process_children(crate::process::children::Args {
					length: None,
					position: None,
					process: process.id().clone(),
					remote: None,
					size: None,
				})
				.await?
			},
			Either::Right(object) => {
				self.command_object_children(crate::object::children::Args {
					object: object.id(&handle).await?.clone(),
				})
				.await?
			},
		}
		Ok(())
	}
}
