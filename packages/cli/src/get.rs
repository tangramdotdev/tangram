use {
	crate::Cli, anstream::eprintln, crossterm::style::Stylize as _, tangram_client as tg,
	tangram_either::Either,
};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<crate::object::get::Format>,

	/// Whether to print blobs.
	#[arg(long)]
	pub print_blobs: bool,

	/// The depth to print.
	#[arg(default_value = "1", long, short = 'd')]
	pub print_depth: crate::object::get::Depth,

	/// Whether to pretty print the output.
	#[arg(long)]
	pub print_pretty: Option<bool>,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		let referent = referent.map(|item| {
			item.map_left(|process| process.id().clone())
				.map_right(|object| object.id().clone())
		});
		eprintln!("{} {referent}", "info".blue().bold());
		match referent.item {
			Either::Left(process) => {
				let args = crate::process::get::Args {
					pretty: args.print_pretty,
					process,
					remote: None,
				};
				self.command_process_get(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::get::Args {
					format: args.format,
					object,
					print_blobs: args.print_blobs,
					print_depth: args.print_depth,
					print_pretty: args.print_pretty,
					remote: None,
				};
				self.command_object_get(args).await?;
			},
		}
		Ok(())
	}
}
