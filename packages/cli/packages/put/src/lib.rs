use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Put an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Put the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[arg(long)]
	pub id: Option<tg::Either<tg::object::Id, tg::process::Id>>,

	#[arg(index = 1)]
	pub input: Option<String>,

	#[arg(long, short)]
	pub kind: Option<tg::object::Kind>,

	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let location = args.location;

	match (args.id, args.kind) {
		(id, kind) if id.is_none() || id.as_ref().is_some_and(tg::Either::is_left) => {
			let args = tangram_cli_object::put::Args {
				bytes: args.bytes,
				id: id.map(tg::Either::unwrap_left),
				input: args.input,
				kind,
				location: location.clone(),
				print: args.print,
			};
			tangram_cli_object::put::run(cli, args).await?;
		},
		(Some(tg::Either::Right(id)), None) => {
			let args = tangram_cli_process::put::Args {
				bytes: args.input,
				id,
				location,
			};
			tangram_cli_process::put::run(cli, args).await?;
		},
		_ => {
			return Err(tg::error!("invalid args"));
		},
	}

	Ok(())
}
