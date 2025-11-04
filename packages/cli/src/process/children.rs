use {
	crate::Cli,
	futures::StreamExt as _,
	tangram_client::{self as tg, prelude::*},
};

/// Get a process's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub length: Option<u64>,

	#[arg(long)]
	pub position: Option<u64>,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,

	#[arg(long)]
	pub size: Option<u64>,
}

impl Cli {
	pub async fn command_process_children(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::process::children::get::Arg {
			position: args.position.map(std::io::SeekFrom::Start),
			length: args.length,
			remote,
			size: args.size,
		};
		let stream = handle.get_process_children(&args.process, arg).await?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
