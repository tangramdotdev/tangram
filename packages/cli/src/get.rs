use {crate::Cli, tangram_client::prelude::*};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
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
					local: args.local,
					object,
					print: args.print,
					remotes: args.remotes,
				};
				self.command_object_get(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::get::Args {
					local: args.local,
					print: args.print,
					process,
					remotes: args.remotes,
				};
				self.command_process_get(args).await?;
			},
		}
		Ok(())
	}
}
