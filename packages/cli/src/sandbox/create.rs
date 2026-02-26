use {crate::Cli, tangram_client::prelude::*};

/// Create a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: super::Options,
}

#[cfg(target_os = "linux")]
const RD_ONLY: u64 = libc::MS_RDONLY as _;

#[cfg(target_os = "macos")]
const RD_ONLY: u64 = libc::MNT_RDONLY as _;

impl Cli {
	pub async fn command_sandbox_create(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let client = handle
			.left()
			.ok_or_else(|| tg::error!("this command requires a client, not a server"))?;

		// Get the host.
		let host = args.options.host.unwrap_or_else(|| tg::host().to_owned());

		// Convert the CLI mounts to the create arg mounts.
		let mounts = args
			.options
			.mounts
			.into_iter()
			.map(|mount| tg::sandbox::create::Mount {
				source: mount.source.map(tg::Either::Left).unwrap(),
				target: mount.target.unwrap(),
				readonly: mount.flags & RD_ONLY != 0,
			})
			.collect();

		// Create the arg.
		let arg = tg::sandbox::create::Arg {
			host,
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
