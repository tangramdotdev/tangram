use {crate::Cli, tangram_client as tg, tangram_either::Either};

/// Put a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub bytes: Option<String>,

	#[arg(long, default_value = "bytes")]
	pub format: Format,

	#[arg(index = 1)]
	pub id: Option<Either<tg::process::Id, tg::object::Id>>,

	#[arg(long, short)]
	pub kind: Option<tg::object::Kind>,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum Format {
	#[default]
	Bytes,
	Json,
}

impl Cli {
	pub async fn command_put(&mut self, args: Args) -> tg::Result<()> {
		match (args.id, args.kind) {
			(Some(Either::Left(id)), None) => {
				let args = crate::process::put::Args {
					bytes: args.bytes,
					id,
				};
				self.command_process_put(args).await?;
			},
			(id, kind) if id.is_none() || id.as_ref().is_some_and(Either::is_right) => {
				let args = crate::object::put::Args {
					bytes: args.bytes,
					format: args.format,
					id: id.map(Either::unwrap_right),
					kind,
				};
				self.command_object_put(args).await?;
			},
			_ => {
				return Err(tg::error!("invalid args"));
			},
		}
		Ok(())
	}
}
