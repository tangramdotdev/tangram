use {crate::Cli, tangram_client::prelude::*};

/// Add a grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub permission: Permission,

	#[command(flatten)]
	pub principal: Principal,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub resource: Resource,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Permission {
	#[arg(
		conflicts_with_all = ["read", "value", "write"],
		long,
		required_unless_present_any = ["read", "value", "write"]
	)]
	pub admin: bool,

	#[arg(
		conflicts_with_all = ["admin", "value", "write"],
		long,
		required_unless_present_any = ["admin", "value", "write"]
	)]
	pub read: bool,

	#[arg(
		conflicts_with_all = ["admin", "read", "write"],
		long = "permission",
		required_unless_present_any = ["admin", "read", "write"]
	)]
	pub value: Option<tg::Permission>,

	#[arg(
		conflicts_with_all = ["admin", "read", "value"],
		long,
		required_unless_present_any = ["admin", "read", "value"]
	)]
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

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Principal {
	#[arg(
		conflicts_with_all = ["group", "user"],
		long,
		required_unless_present_any = ["group", "user"]
	)]
	pub all: bool,

	#[arg(
		conflicts_with_all = ["all", "user"],
		long,
		required_unless_present_any = ["all", "user"]
	)]
	pub group: Option<String>,

	#[arg(
		conflicts_with_all = ["all", "group"],
		long,
		required_unless_present_any = ["all", "group"]
	)]
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Resource {
	#[arg(conflicts_with = "tag", long, required_unless_present = "tag")]
	pub namespace: Option<tg::Namespace>,

	#[arg(
		conflicts_with = "namespace",
		long,
		required_unless_present = "namespace"
	)]
	pub tag: Option<tg::Tag>,
}

impl Cli {
	pub async fn command_grant(&mut self, args: Args) -> tg::Result<()> {
		match (args.resource.namespace, args.resource.tag) {
			(Some(namespace), None) => {
				self.command_namespace_grants_add(crate::namespace::grants::add::Args {
					namespace,
					location: args.location,
					permission: args.permission,
					principal: args.principal,
					print: args.print,
				})
				.await?;
			},
			(None, Some(tag)) => {
				self.command_tag_grants_add(crate::tag::grants::add::Args {
					location: args.location,
					permission: args.permission,
					principal: args.principal,
					print: args.print,
					tag,
				})
				.await?;
			},
			_ => return Err(tg::error!("expected exactly one of --namespace or --tag")),
		}
		Ok(())
	}
}
