use {crate::Cli, tangram_client::prelude::*};

/// Put a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Put the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[arg(index = 1)]
	pub input: Option<String>,

	#[arg(long)]
	pub id: Option<tg::Either<tg::object::Id, tg::process::Id>>,

	#[arg(long, short)]
	pub kind: Option<tg::object::Kind>,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_put(&mut self, args: Args) -> tg::Result<()> {
		match (args.id, args.kind) {
			(id, kind) if id.is_none() || id.as_ref().is_some_and(tg::Either::is_left) => {
				let args = crate::object::put::Args {
					bytes: args.bytes,
					id: id.map(tg::Either::unwrap_left),
					input: args.input,
					kind,
					local: args.local,
					print: args.print,
					remotes: args.remotes,
				};
				self.command_object_put(args).await?;
			},
			(Some(tg::Either::Right(id)), None) => {
				let args = crate::process::put::Args {
					bytes: args.input,
					id,
					local: args.local,
					remotes: args.remotes,
				};
				self.command_process_put(args).await?;
			},
			_ => {
				return Err(tg::error!("invalid args"));
			},
		}
		Ok(())
	}
}
