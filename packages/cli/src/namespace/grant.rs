use {crate::Cli, tangram_client::prelude::*};

/// Grant permission on a namespace.
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

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_namespace_grant(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let grant = client
			.grant_namespace_permission(tg::namespace::grant::Arg {
				namespace: args.namespace.clone(),
				user: args.user,
				group: args.group,
				public: args.public,
				permission: args.permission,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to grant the namespace permission"),
			)?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
