use {crate::Cli, tangram_client::prelude::*};

pub use crate::run::Args;

impl Cli {
	pub async fn command_build(&mut self, args: crate::run::Args) -> tg::Result<()> {
		let detach = args.options.detach;
		let verbose = args.options.verbose;
		let checkout = args.options.checkout.is_some();
		let print = args.options.print.clone();
		let local = args.options.spawn.local.get();
		let remotes = args.options.spawn.remotes.get();

		// Build.
		let output = self.build(args).await?;

		// Print the output.
		if detach && !verbose {
			let string = output
				.try_unwrap_string()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?;
			Self::print_display(string);
		} else if checkout {
			Self::print_display(output);
		} else if (detach && verbose) || !output.is_null() {
			let arg = tg::object::get::Arg {
				local,
				metadata: false,
				remotes,
			};
			self.print_value(&output, print, arg).await?;
		}

		Ok(())
	}

	pub(crate) async fn build(&mut self, mut args: Args) -> tg::Result<tg::Value> {
		if args.options.spawn.sandbox.get().is_some_and(|v| !v) {
			return Err(tg::error!("a build must be sandboxed"));
		}
		args.options.spawn.sandbox = crate::process::spawn::Sandbox::new(Some(true));

		match args.options.spawn.stdin {
			None => {
				args.options.spawn.stdin = Some(tg::process::Stdio::Null);
			},
			Some(tg::process::Stdio::Blob(_) | tg::process::Stdio::Null) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdin for a build"));
			},
		}

		match args.options.spawn.stdout {
			None => {
				args.options.spawn.stdout = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Log) => (),
			Some(_) => {
				return Err(tg::error!("invalid stdout for a build"));
			},
		}

		match args.options.spawn.stderr {
			None => {
				args.options.spawn.stderr = Some(tg::process::Stdio::Log);
			},
			Some(tg::process::Stdio::Log) => (),
			Some(_) => {
				return Err(tg::error!("invalid stderr for a build"));
			},
		}

		if matches!(
			args.options.spawn.tty.tty.as_ref(),
			Some(tg::Either::Left(true) | tg::Either::Right(_))
		) {
			return Err(tg::error!("invalid tty for a build"));
		}
		args.options.spawn.tty = crate::process::spawn::Tty::new_disabled();

		if args.options.view.is_none() {
			args.options.view = Some(crate::run::View::Inline);
		}

		let output = self.run(args).await?;

		Ok(output)
	}
}
