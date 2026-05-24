use {crate::Cli, tangram_client::prelude::*};

/// Add a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub all: bool,

	#[arg(long)]
	pub group: Option<String>,

	#[arg(long)]
	pub namespace: Option<tg::Namespace>,

	#[command(flatten)]
	pub permission: Permission,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub tag: Option<tg::Tag>,

	#[arg(long)]
	pub user: Option<String>,
}

/// Delete a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct RevokeArgs {
	#[arg(long)]
	pub all: bool,

	#[arg(long)]
	pub group: Option<String>,

	#[arg(long)]
	pub namespace: Option<tg::Namespace>,

	#[command(flatten)]
	pub permission: Permission,

	#[arg(long)]
	pub tag: Option<tg::Tag>,

	#[arg(long)]
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Permission {
	#[arg(long)]
	pub admin: bool,

	#[arg(long)]
	pub read: bool,

	#[arg(long = "permission")]
	pub value: Option<tg::Permission>,

	#[arg(long)]
	pub write: bool,
}

impl Permission {
	pub(crate) fn get(&self) -> tg::Result<tg::Permission> {
		let mut permissions = Vec::new();
		if let Some(permission) = self.value {
			permissions.push(permission);
		}
		if self.read {
			permissions.push(tg::Permission::Read);
		}
		if self.write {
			permissions.push(tg::Permission::Write);
		}
		if self.admin {
			permissions.push(tg::Permission::Admin);
		}
		match permissions.as_slice() {
			[permission] => Ok(*permission),
			_ => Err(tg::error!("expected exactly one permission")),
		}
	}
}

impl Cli {
	pub async fn command_grant(&mut self, args: Args) -> tg::Result<()> {
		match (args.namespace, args.tag) {
			(Some(namespace), None) => {
				self.command_namespace_grants_add(crate::namespace::grants::add::Args {
					all: args.all,
					group: args.group,
					namespace,
					permission: args.permission,
					print: args.print,
					user: args.user,
				})
				.await?;
			},
			(None, Some(tag)) => {
				self.command_tag_grants_add(crate::tag::grants::add::Args {
					all: args.all,
					group: args.group,
					permission: args.permission,
					print: args.print,
					tag,
					user: args.user,
				})
				.await?;
			},
			_ => return Err(tg::error!("expected exactly one of --namespace or --tag")),
		}
		Ok(())
	}

	pub async fn command_revoke(&mut self, args: RevokeArgs) -> tg::Result<()> {
		match (args.namespace, args.tag) {
			(Some(namespace), None) => {
				self.command_namespace_grants_delete(crate::namespace::grants::delete::Args {
					all: args.all,
					group: args.group,
					namespace,
					permission: args.permission,
					user: args.user,
				})
				.await?;
			},
			(None, Some(tag)) => {
				self.command_tag_grants_delete(crate::tag::grants::delete::Args {
					all: args.all,
					group: args.group,
					permission: args.permission,
					tag,
					user: args.user,
				})
				.await?;
			},
			_ => return Err(tg::error!("expected exactly one of --namespace or --tag")),
		}
		Ok(())
	}
}
