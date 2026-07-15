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
		let is_process = matches!(
			referent.item(),
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map(|item| match item {
				tg::get::Item::Id(id) => id.try_into(),
				tg::get::Item::Pointer(_) => unreachable!(),
			})?;
			let args = crate::process::touch::Args { locations, process };
			self.command_process_touch(args).await?;
		} else {
			let object = referent.try_map(|item| match item {
				tg::get::Item::Id(id) => id.try_into(),
				tg::get::Item::Pointer(_) => Err(tg::error!("expected an object or process id")),
			})?;
			let args = crate::object::touch::Args { locations, object };
			self.command_object_touch(args).await?;
		}

		Ok(())
	}
}
