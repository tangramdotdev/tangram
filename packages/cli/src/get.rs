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
		let item = match referent.item() {
			tg::Either::Left(tg::graph::Edge::Object(object)) => tg::Either::Left(object.id()),
			tg::Either::Left(tg::graph::Edge::Pointer(_)) => {
				return Err(tg::error!("expected an object, got a pointer"));
			},
			tg::Either::Right(process) => {
				let id = process
					.id()
					.right()
					.ok_or_else(|| tg::error!("expected a process id"))?
					.clone();
				tg::Either::Right(id)
			},
		};
		let referent = referent.map(|_| item);
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
