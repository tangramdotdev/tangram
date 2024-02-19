use crate::{util::build_or_object_id, Cli};
use either::Either;
use tangram_client as tg;
use tangram_error::Result;

/// Push a build or an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[clap(value_parser = build_or_object_id)]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl Cli {
	pub async fn command_push(&self, args: Args) -> Result<()> {
		match args.id {
			Either::Left(id) => {
				self.command_build_push(super::build::PushArgs { id })
					.await?;
			},
			Either::Right(id) => {
				self.command_object_push(super::object::PushArgs { id })
					.await?;
			},
		}
		Ok(())
	}
}
