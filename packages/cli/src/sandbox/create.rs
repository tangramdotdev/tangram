use {crate::Cli, std::path::Path, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: super::Options,
}

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let client = handle
			.left()
			.ok_or_else(|| tg::error!("this command requires a client, not a server"))?;

		// Convert the CLI mounts to the create arg mounts.
		let mounts = args
			.options
			.mounts
			.into_iter()
			.map(|mount| {
				tg::Either::Left(tg::process::data::Mount {
					source: mount.source.unwrap_or_else(|| Path::new("/").to_owned()),
					target: mount.target.unwrap_or_else(|| Path::new("/").to_owned()),
					readonly: mount.flags & libc::MS_RDONLY != 0,
				})
			})
			.collect();

		// Create the arg.
		let arg = tg::sandbox::create::Arg {
			hostname: args.options.hostname,
			mounts,
			network: args.options.network.get(),
			user: args.options.user,
		};

		// Create the sandbox.
		let output = client
			.create_sandbox(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;

		// Print the sandbox ID.
		Self::print_display(&output.id);

		Ok(())
	}
}
