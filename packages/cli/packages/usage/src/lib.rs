use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get usage for a user or organization.
///
/// Usage includes all regions of the selected server unless a specific region is selected with --location.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub period: PeriodArgs,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub selector: Option<tg::Referent<tg::Selector<tg::Id>>>,
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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let location = args.location.get();
	let mut arg = tg::usage::Arg::from(args.period);
	arg.location = location;
	let usage = if let Some(selector) = args.selector {
		arg.location = args.location.get_for_options(&selector);
		arg.tokens = selector.options.tokens;
		match selector.node {
			tg::Selector::Id(id) => match id.kind() {
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
							Some(&tg::user::Selector::Id(id.try_into()?)),
							arg.clone(),
						)
						.await?
				},
				_ => return Err(tg::error!("expected a user or organization selector")),
			},
			tg::Selector::Specifier(specifier) => {
				let user = tg::user::Selector::Specifier(specifier.clone());
				let usage = client.try_get_user_usage(Some(&user), arg.clone()).await?;
				if usage.is_some() {
					usage
				} else {
					let organization = tg::organization::Selector::Specifier(specifier);
					client
						.try_get_organization_usage(&organization, arg)
						.await?
				}
			},
		}
	} else {
		client.try_get_user_usage(None, arg).await?
	};
	let usage = usage.ok_or_else(|| tg::error!("failed to find the usage account"))?;
	cli.print_serde(usage, args.print).await?;

	Ok(())
}

impl From<PeriodArgs> for tg::usage::Arg {
	fn from(value: PeriodArgs) -> Self {
		Self {
			day: value.day,
			hour: value.hour,
			location: None,
			month: value.month,
			tokens: tg::authorization::Tokens::default(),
			week: value.week,
		}
	}
}
