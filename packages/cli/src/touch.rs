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
		let item = match referent.item {
			tg::Either::Left(edge) => tg::Either::Left(
				edge.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id(),
			),
			tg::Either::Right(process) => {
				let id = process
					.id()
					.right()
					.ok_or_else(|| tg::error!("expected a process id"))?
					.clone();
				tg::Either::Right(id)
			},
		};

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
