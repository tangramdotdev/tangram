use {crate::Cli, tangram_client::prelude::*};

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let explicit_location = args.locations.get();
		let reference_location = args.process.options().location.clone();
		let mut options = args.process.options().clone();
		if let Some(location) = explicit_location.clone() {
			options.location = Some(location);
		}
		let reference = tg::Reference::with_node_and_options(args.process.node().clone(), options);
		let process = self.resolve(&reference).await?.try_map(|node| match node {
			tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected a process")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected a process")),
		})?;
		let location = explicit_location
			.or_else(|| process.options.location.clone().map(Into::into))
			.or(reference_location);
		self.command_process_wait_with_referent(process, location, args.print)
			.await
	}

	pub(crate) async fn command_process_wait_with_referent(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		location: Option<tg::location::Arg>,
		print: crate::print::Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = process.node.clone();
		let process = tg::Process::<tg::Value>::with_referent(process);
		let arg = tg::process::wait::Arg {
			lease: None,
			location,
			tokens: tg::authorization::Tokens::default(),
		};
		let output = process
			.wait_with_handle(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
		self.print_serde(output.to_data(), print).await?;
		Ok(())
	}
}
