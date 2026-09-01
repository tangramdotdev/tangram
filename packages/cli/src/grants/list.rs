use {crate::Cli, tangram_client::prelude::*};

/// List grants.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub output: crate::print::OutputOptions,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// List the grants on this resource.
	#[arg(long)]
	pub resource: Option<tg::Selector<tg::Id>>,

	/// List the grants held by this subject.
	#[arg(conflicts_with = "resource", long)]
	pub subject: Option<tg::authorization::subject::Selector>,
}

impl Cli {
	pub async fn command_grants_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let subject = args.subject.is_some();
		let arg = tg::grant::list::Arg {
			location: args.location.get(),
			resource: args.resource,
			subject: args.subject,
		};
		let output = client
			.list_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the grants"))?
			.ok_or_else(|| {
				if subject {
					tg::error!("failed to find the subject")
				} else {
					tg::error!("failed to find the resource")
				}
			})?;
		if args.output.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.data, args.print).await?;
		}
		Ok(())
	}
}
