use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Push an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub id: tg::Id,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_push(&self, args: Args) -> Result<()> {
		let client = &self.client().await?;

		#[allow(clippy::same_functions_in_if_condition)]
		if let Ok(id) = args.id.clone().try_into() {
			client.push_build(None, &id).await?;
		} else if let Ok(id) = args.id.clone().try_into() {
			client.push_object(&id).await?;
		} else {
			return Err(error!("Expected a build ID or an object ID."));
		}

		Ok(())
	}
}
