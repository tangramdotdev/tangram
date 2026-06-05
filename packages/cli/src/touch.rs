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

		let referent = self.get_resolved_reference(&args.reference).await?;
		let item = match referent.item {
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => {
				tg::Either::Right(id.try_into()?)
			},
			tg::get::Item::Id(id) => tg::Either::Left(id.try_into()?),
			tg::get::Item::Pointer(_) => {
				return Err(tg::error!("expected an object or process id"));
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
