use {crate::Cli, tangram_client::prelude::*};

/// Delete a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub permissions: tg::grant::Set,

	#[arg(index = 1)]
	pub principal: tg::principal::Selector,

	#[arg(index = 3)]
	pub resource: tg::grant::Resource,
}

impl Cli {
	pub async fn command_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::grant::delete::Arg {
			principal: args.principal.clone(),
			permissions: args.permissions,
			resource: args.resource.clone(),
		};
		client
			.delete_grant(arg)
			.await
			.map_err(
				|error| tg::error!(!error, principal = %args.principal, resource = %args.resource, "failed to delete the grant"),
			)?
			.ok_or_else(|| tg::error!("failed to find the grant"))?;
		Ok(())
	}
}
