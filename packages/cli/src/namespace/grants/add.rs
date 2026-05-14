use {crate::Cli, tangram_client::prelude::*};

/// Add a namespace grant.
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
	pub async fn command_namespace_grants_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let grant = client
			.create_namespace_grant(tg::namespace::grants::create::Arg {
				namespace: args.namespace.clone(),
				user: args.user,
				group: args.group,
				public: args.public,
				permission: args.permission,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to add the namespace grant"),
			)?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
