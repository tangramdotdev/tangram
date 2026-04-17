use {crate::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Locations,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = referent
			.item
			.map_left(|object| object.id().clone())
			.map_right(|process| process.id().clone());

		match item {
			tg::Either::Left(object) => {
				let args = crate::object::metadata::Args {
					locations: locations.clone(),
					object,
					print,
				};
				self.command_object_metadata(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::metadata::Args {
					locations,
					print,
					process,
				};
				self.command_process_metadata(args).await?;
			},
		}

		Ok(())
	}
}
