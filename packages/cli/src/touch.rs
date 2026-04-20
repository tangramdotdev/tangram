use {crate::Cli, tangram_client::prelude::*};

/// Touch an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_touch(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;

		let referent = self.get_reference(&args.reference).await?;
		let item = referent
			.item
			.map_left(|object| object.id().clone())
			.map_right(|process| process.id().clone());

		match item {
			tg::Either::Left(object) => {
				let args = crate::object::touch::Args {
					locations: locations.clone(),
					object,
				};
				self.command_object_touch(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::touch::Args { locations, process };
				self.command_process_touch(args).await?;
			},
		}

		Ok(())
	}
}
