use {crate::Cli, tangram_client::prelude::*};

/// Get usage for a user or organization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub period: PeriodArgs,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub selector: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct PeriodArgs {
	/// Get usage for a UTC day in YYYY-MM-DD format.
	#[arg(long, conflicts_with_all = ["hour", "month", "week"])]
	pub day: Option<String>,

	/// Get usage for a UTC hour as an hour-aligned RFC 3339 timestamp.
	#[arg(long, conflicts_with_all = ["day", "month", "week"])]
	pub hour: Option<String>,

	/// Get usage for a UTC month in YYYY-MM format.
	#[arg(long, conflicts_with_all = ["day", "hour", "week"])]
	pub month: Option<String>,

	/// Get usage for an ISO week in YYYY-Www format.
	#[arg(long, conflicts_with_all = ["day", "hour", "month"])]
	pub week: Option<String>,
}

impl Cli {
	pub async fn command_usage(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::usage::Arg::from(args.period);
		let usage = if let Some(selector) = args.selector {
			if let Ok(id) = selector.parse::<tg::Id>() {
				match id.kind() {
					tg::id::Kind::Organization => {
						client
							.try_get_organization_usage(
								&tg::organization::Selector::Id(id.try_into()?),
								arg.clone(),
							)
							.await?
					},
					tg::id::Kind::User => {
						client
							.try_get_user_usage(
								&tg::user::Selector::Id(id.try_into()?),
								arg.clone(),
							)
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
					client
						.try_get_user_usage(&user_selector, arg.clone())
						.await?
				} else {
					client
						.try_get_organization_usage(
							&tg::organization::Selector::Specifier(specifier),
							arg.clone(),
						)
						.await?
				}
			}
		} else {
			let user = client
				.get_current_user(tg::user::current::Arg::default())
				.await?
				.ok_or_else(|| tg::error!("not logged in"))?;
			client
				.try_get_user_usage(&tg::user::Selector::Id(user.id), arg)
				.await?
		};
		let usage = usage.ok_or_else(|| tg::error!("failed to find the usage account"))?;
		self.print_serde(usage, args.print).await?;

		Ok(())
	}
}

impl From<PeriodArgs> for tg::usage::Arg {
	fn from(value: PeriodArgs) -> Self {
		Self {
			day: value.day,
			hour: value.hour,
			month: value.month,
			week: value.week,
		}
	}
}
