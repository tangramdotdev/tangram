use {crate::Cli, tangram_client::prelude::*};

/// Delete a namespace grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[command(flatten)]
	pub principal: crate::grant::Principal,
}

impl Cli {
	pub async fn command_namespace_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		let principal = args.principal.resolve(&client).await?;
		client
			.delete_namespace_grant(tg::namespace::grants::delete::Arg {
				location: args.location.get(),
				namespace: args.namespace.clone(),
				permission,
				principal,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to delete the namespace grant"),
			)?
			.ok_or_else(|| tg::error!("failed to find the namespace grant"))?;
		Ok(())
	}
}
