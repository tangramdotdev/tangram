use {crate::Cli, tangram_client::prelude::*};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The object or process.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[arg(long)]
	pub wait: bool,
}

impl Cli {
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;
		let wait = args.wait;

		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			tg::Either::Left(edge) => {
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id();
				let args = crate::object::children::Args {
					locations: locations.clone(),
					object,
					print,
				};
				self.command_object_children(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::children::Args {
					length: None,
					locations,
					position: None,
					print,
					process: process.id().unwrap_right().clone(),
					size: None,
					wait,
				};
				self.command_process_children(args).await?;
			},
		}
		Ok(())
	}
}
