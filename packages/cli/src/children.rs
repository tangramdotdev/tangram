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
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			Either::Left(process) => {
				let args = crate::process::children::Args {
					length: None,
					position: None,
					process: process.id().clone(),
					remote: None,
					size: None,
				};
				self.command_process_children(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::children::Args {
					object: object.id(),
				};
				self.command_object_children(args).await?;
			},
		}
		Ok(())
	}
}
