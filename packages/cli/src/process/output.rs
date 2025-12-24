use {crate::Cli, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let process = &args.process;
		let arg = tg::process::wait::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let wait = handle.wait_process(process, arg).await.map_err(
			|source| tg::error!(!source, id = %process, "failed to wait for the process"),
		)?;
		if let Some(error) = wait.error {
			let error = error
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
			let error = tg::Error::with_object(tg::error::Object {
				message: Some("the process failed".to_owned()),
				source: Some(tg::Referent::with_item(error)),
				..Default::default()
			});
			return Err(error);
		}
		if wait.exit > 1 && wait.exit < 128 {
			return Err(tg::error!("the process exited with code {}", wait.exit));
		}
		if wait.exit >= 128 {
			return Err(tg::error!(
				"the process exited with signal {}",
				wait.exit - 128
			));
		}
		self.print_serde(&wait.output, args.print).await?;
		Ok(())
	}
}
