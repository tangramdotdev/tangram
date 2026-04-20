use {crate::Cli, tangram_client::prelude::*};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the metadata.
	#[arg(long)]
	pub metadata: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;
		let referent = self.get_reference(&args.reference).await?;
		let referent = referent.map(|item| {
			item.map_left(|object| object.id().clone())
				.map_right(|process| process.id().clone())
		});
		Self::print_info_message(&referent.to_string());
		match referent.item {
			tg::Either::Left(object) => {
				let args = crate::object::get::Args {
					bytes: args.bytes,
					locations: locations.clone(),
					metadata: args.metadata,
					object,
					print,
				};
				self.command_object_get(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::get::Args {
					locations,
					metadata: args.metadata,
					print,
					process,
				};
				self.command_process_get(args).await?;
			},
		}
		Ok(())
	}
}
