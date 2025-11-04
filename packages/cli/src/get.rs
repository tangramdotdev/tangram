use {crate::Cli, tangram_client as tg, tangram_either::Either};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	/// The remote to get from.
	#[arg(long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		let referent = referent.map(|item| {
			item.map_left(|process| process.id().clone())
				.map_right(|object| object.id().clone())
		});
		Self::print_info_message(&referent.to_string());
		match referent.item {
			Either::Left(process) => {
				let args = crate::process::get::Args {
					print: args.print,
					process,
					remote: args.remote,
				};
				self.command_process_get(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::get::Args {
					bytes: args.bytes,
					object,
					print: args.print,
					remote: args.remote,
				};
				self.command_object_get(args).await?;
			},
		}
		Ok(())
	}
}
