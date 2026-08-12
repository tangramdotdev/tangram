use {crate::Cli, tangram_client::prelude::*};

/// Delete a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub permissions: tg::Either<tg::authorization::permission::Set, String>,

	#[arg(index = 3)]
	pub resource: tg::Referent<tg::Selector<tg::Id>>,

	#[arg(index = 1)]
	pub subject: tg::authorization::subject::Selector,
}

impl Cli {
	pub async fn command_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::grant::delete::Arg {
			permissions: args.permissions.clone(),
			resource: args.resource.clone(),
			subject: args.subject.clone(),
		};
		client
			.delete_grant(arg)
			.await
			.map_err(
				|error| tg::error!(!error, resource = %args.resource, subject = %args.subject, "failed to delete the grant"),
			)?
			.ok_or_else(|| tg::error!("failed to find the grant"))?;
		Ok(())
	}
}
