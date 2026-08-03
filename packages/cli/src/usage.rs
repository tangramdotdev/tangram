use {crate::Cli, tangram_client::prelude::*};

/// Get storage usage for a user or organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub selector: Option<String>,
}

impl Cli {
	pub async fn command_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let usage = if let Some(selector) = args.selector {
			if let Ok(id) = selector.parse::<tg::Id>() {
				match id.kind() {
					tg::id::Kind::Organization => {
						client
							.try_get_organization_usage(&tg::organization::Selector::Id(
								id.try_into()?,
							))
							.await?
					},
					tg::id::Kind::User => {
						client
							.try_get_user_usage(&tg::user::Selector::Id(id.try_into()?))
							.await?
					},
					_ => return Err(tg::error!("expected a user or organization selector")),
				}
			} else {
				let specifier: tg::Specifier = selector.parse()?;
				let user_selector = tg::user::Selector::Specifier(specifier.clone());
				if client
					.try_get_user(&user_selector, tg::user::get::Arg::default())
					.await?
					.is_some()
				{
					client.try_get_user_usage(&user_selector).await?
				} else {
					client
						.try_get_organization_usage(&tg::organization::Selector::Specifier(
							specifier,
						))
						.await?
				}
			}
		} else {
			let user = client
				.get_current_user(tg::user::current::Arg::default())
				.await?
				.ok_or_else(|| tg::error!("not logged in"))?;
			client
				.try_get_user_usage(&tg::user::Selector::Id(user.id))
				.await?
		};
		let usage = usage.ok_or_else(|| tg::error!("failed to find the storage owner"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}
