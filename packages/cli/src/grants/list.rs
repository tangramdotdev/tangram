use {crate::Cli, tangram_client::prelude::*};

/// List grants.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub resource: tg::grant::Resource,
}

impl Cli {
	pub async fn command_grants_list(&mut self, args: Args) -> tg::Result<()> {
		match args.resource {
			tg::grant::Resource::Id(id) => self.command_grants_list_id(id, args.print).await?,
			tg::grant::Resource::Specifier(specifier) => {
				let reference = tg::Reference::with_item(tg::reference::Item::Specifier(
					tg::specifier::Pattern::from(specifier.clone()),
				));
				let referent = self.get_reference(&reference).await?;
				let tg::get::Item::Id(id) = referent.item else {
					return Err(tg::error!("unsupported grant resource"));
				};
				self.command_grants_list_id(id, args.print).await?;
			},
		}
		Ok(())
	}

	async fn command_grants_list_id(
		&mut self,
		id: tg::Id,
		print: crate::print::Options,
	) -> tg::Result<()> {
		match id.kind() {
			tg::id::Kind::User => {
				let args = crate::user::grants::Args {
					location: crate::location::Args::default(),
					parent: None,
					print,
					user: tg::Selector::Id(id.try_into()?),
				};
				self.command_user_grants(args).await?;
			},
			tg::id::Kind::Group => {
				let args = crate::group::grants::Args {
					group: tg::Selector::Id(id.try_into()?),
					location: crate::location::Args::default(),
					print,
				};
				self.command_group_grants(args).await?;
			},
			tg::id::Kind::Organization => {
				let args = crate::organization::grants::Args {
					location: crate::location::Args::default(),
					organization: tg::Selector::Id(id.try_into()?),
					print,
				};
				self.command_organization_grants(args).await?;
			},
			tg::id::Kind::Tag => {
				let args = crate::tag::grants::Args {
					location: crate::location::Args::default(),
					print,
					tag: tg::Selector::Id(id.try_into()?),
				};
				self.command_tag_grants(args).await?;
			},
			_ => return Err(tg::error!("unsupported grant resource")),
		}
		Ok(())
	}
}
