use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = referent
			.item
			.map_left(|process| process.id().clone())
			.map_right(|object| object.id().clone());

		match item {
			Either::Left(process) => {
				let args = crate::process::metadata::Args {
					process,
					pretty: args.pretty,
				};
				self.command_process_metadata(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::metadata::Args {
					object,
					pretty: args.pretty,
				};
				self.command_object_metadata(args).await?;
			},
		}

		Ok(())
	}
}
