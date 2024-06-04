use crate::Cli;
use tangram_client as tg;

/// Push a build or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub arg: Arg,

	#[arg(short, long)]
	pub remote: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Build(tg::build::Id),
	Object(tg::object::Id),
}

impl Cli {
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		match args.arg {
			Arg::Build(arg) => {
				self.command_build_push(super::build::push::Args {
					build: arg,
					logs: false,
					recursive: false,
					remote: args.remote,
					targets: false,
				})
				.await?;
			},
			Arg::Object(arg) => {
				self.command_object_push(super::object::push::Args {
					object: arg,
					remote: args.remote,
				})
				.await?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(build) = s.parse() {
			return Ok(Arg::Build(build));
		}
		if let Ok(object) = s.parse() {
			return Ok(Arg::Object(object));
		}
		Err(tg::error!(%s, "expected a build or an object"))
	}
}
