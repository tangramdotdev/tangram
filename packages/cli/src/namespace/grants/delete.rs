use {crate::Cli, tangram_client::prelude::*};

/// Delete a namespace grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub all: bool,

	#[arg(long)]
	pub group: Option<String>,

	#[arg(long)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub async fn command_namespace_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		client
			.delete_namespace_grant(tg::namespace::grants::delete::Arg {
				all: args.all,
				group: args.group,
				namespace: args.namespace.clone(),
				permission,
				user: args.user,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to delete the namespace grant"),
			)?
			.ok_or_else(|| tg::error!("failed to find the namespace grant"))?;
		Ok(())
	}
}
