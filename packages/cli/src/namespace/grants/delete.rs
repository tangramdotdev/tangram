use {crate::Cli, tangram_client::prelude::*};

/// Delete a namespace grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,

	#[arg(index = 2)]
	pub permission: tg::Permission,

	#[arg(long)]
	pub user: Option<String>,

	#[arg(long)]
	pub group: Option<String>,

	#[arg(long)]
	pub public: bool,
}

impl Cli {
	pub async fn command_namespace_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.delete_namespace_grant(tg::namespace::grants::delete::Arg {
				namespace: args.namespace.clone(),
				user: args.user,
				group: args.group,
				public: args.public,
				permission: args.permission,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to delete the namespace grant"),
			)?
			.ok_or_else(|| tg::error!("failed to find the namespace grant"))?;
		Ok(())
	}
}
