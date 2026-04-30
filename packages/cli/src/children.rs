use {crate::Cli, tangram_client::prelude::*};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	/// The object or process.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;

		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			tg::Either::Left(edge) => {
				let object = match edge {
					tg::graph::Edge::Object(object) => object.id(),
					tg::graph::Edge::Pointer(_) => {
						return Err(tg::error!("expected an object, got a pointer"));
					},
				};
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
				};
				self.command_process_children(args).await?;
			},
		}
		Ok(())
	}
}
