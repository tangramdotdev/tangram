use {crate::Cli, tangram_client::prelude::*};

/// Create a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub permission: tg::grant::Permission,

	#[arg(index = 1)]
	pub principal: tg::grant::Principal,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 3)]
	pub resource: tg::grant::Resource,
}

impl Cli {
	pub async fn command_grants_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::grant::create::Arg {
			principal: args.principal.clone(),
			permission: args.permission,
			resource: args.resource.clone(),
		};
		let grant = client
			.create_grant(arg)
			.await
			.map_err(
				|error| tg::error!(!error, principal = %args.principal, resource = %args.resource, "failed to create the grant"),
			)?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
