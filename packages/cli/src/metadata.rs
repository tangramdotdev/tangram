use {crate::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

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
		match referent.item {
			tg::Either::Left(edge) => {
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id();
				let args = crate::object::metadata::Args {
					locations: locations.clone(),
					object,
					print,
				};
				self.command_object_metadata(args).await?;
			},
			tg::Either::Right(process) => {
				let process = process
					.id()
					.right()
					.ok_or_else(|| tg::error!("expected a process id"))?
					.clone();
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
