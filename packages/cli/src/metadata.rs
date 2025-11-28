use {crate::Cli, tangram_client::prelude::*, tangram_either::Either};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = referent
			.item
			.map_left(|object| object.id().clone())
			.map_right(|process| process.id().clone());

		match item {
			Either::Left(object) => {
				let args = crate::object::metadata::Args {
					object,
					print: args.print,
					remote: None,
				};
				self.command_object_metadata(args).await?;
			},
			Either::Right(process) => {
				let args = crate::process::metadata::Args {
					process,
					print: args.print,
					remote: None,
				};
				self.command_process_metadata(args).await?;
			},
		}

		Ok(())
	}
}
