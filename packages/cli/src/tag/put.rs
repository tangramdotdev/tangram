use {crate::Cli, tangram_client::prelude::*};

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(long, short)]
	pub force: bool,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(default_value = ".", index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub tag: Option<tg::Tag>,
}

impl Cli {
	pub async fn command_tag_put(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the reference.
		let arg = tg::get::Arg {
			checkin: args.checkin.to_options(),
			..Default::default()
		};
		let referent = self.get_reference_with_arg(&args.reference, arg).await?;
		let item = match referent.item {
			tg::Either::Left(edge) => tg::Either::Left(
				edge.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?
					.id(),
			),
			tg::Either::Right(process) => tg::Either::Right(
				process
					.id()
					.right()
					.ok_or_else(|| tg::error!("expected a process id"))?
					.clone(),
			),
		};

		// Put the tag.
		let arg = tg::tag::put::Arg {
			force: args.force,
			item,
			location: args.location.get(),
			replicate: false,
		};
		let tag = args.tag.unwrap();
		client
			.put_tag(&tag, arg)
			.await
			.map_err(|source| tg::error!(!source, %tag, "failed to put the tag"))?;

		Ok(())
	}
}
