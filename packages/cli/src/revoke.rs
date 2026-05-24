use {crate::Cli, tangram_client::prelude::*};

/// Delete a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[command(flatten)]
	pub principal: crate::grant::Principal,

	#[command(flatten)]
	pub resource: crate::grant::Resource,
}

impl Cli {
	pub async fn command_revoke(&mut self, args: Args) -> tg::Result<()> {
		match (args.resource.namespace, args.resource.tag) {
			(Some(namespace), None) => {
				self.command_namespace_grants_delete(crate::namespace::grants::delete::Args {
					namespace,
					location: args.location,
					permission: args.permission,
					principal: args.principal,
				})
				.await?;
			},
			(None, Some(tag)) => {
				self.command_tag_grants_delete(crate::tag::grants::delete::Args {
					location: args.location,
					permission: args.permission,
					principal: args.principal,
					tag,
				})
				.await?;
			},
			_ => return Err(tg::error!("expected exactly one of --namespace or --tag")),
		}
		Ok(())
	}
}
