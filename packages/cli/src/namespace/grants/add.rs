use {crate::Cli, tangram_client::prelude::*};

/// Add a namespace grant.
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

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_namespace_grants_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		let principal = args.principal.resolve(&client).await?;
		let grant = client
			.create_namespace_grant(tg::namespace::grants::create::Arg {
				location: args.location.get(),
				namespace: args.namespace.clone(),
				permission,
				principal,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to add the namespace grant"),
			)?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
