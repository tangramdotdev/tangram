use {crate::Cli, tangram_client::prelude::*};

pub use crate::process::run::{Args, Options};

impl Cli {
	pub async fn command_build(&mut self, args: Args) -> tg::Result<()> {
		let options = args.options.clone();
		let output = self.build(args).await?;
		self.print_build_output(&options, output).await
	}

	pub(crate) async fn print_build_output(
		&mut self,
		options: &Options,
		output: tg::Value,
	) -> tg::Result<()> {
		let location = options.spawn.location.get();
		if options.detach && !options.verbose {
			let string = output
				.try_unwrap_string()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?;
			Self::print_display(string);
		} else if options.checkout.is_some() {
			Self::print_display(output);
		} else if (options.detach && options.verbose) || !output.is_null() {
			let arg = tg::object::get::Arg {
				location,
				metadata: false,
				stored: false,
				tokens: tg::grant::Tokens::default(),
			};
			self.print_value(&output, options.print.clone(), arg)
				.await?;
		}

		Ok(())
	}

	pub(crate) async fn build(&mut self, mut args: Args) -> tg::Result<tg::Value> {
		match args.options.spawn.sandbox.get() {
			Some(tg::Either::Left(false)) => {
				return Err(tg::error!("a build must be sandboxed"));
			},
			Some(tg::Either::Right(_)) => {
				return Err(tg::error!("a build must not use an existing sandbox"));
			},
			_ => (),
		}
		args.options.spawn.sandbox.set(Some(tg::Either::Left(true)));

		if args.options.spawn.stdin.is_none() {
			args.options.spawn.stdin = Some(tg::process::Stdio::Null);
		}

		if args.options.spawn.stdout.is_none() {
			args.options.spawn.stdout = Some(tg::process::Stdio::Log);
		}

		if args.options.spawn.stderr.is_none() {
			args.options.spawn.stderr = Some(tg::process::Stdio::Log);
		}

		let cacheable = args.options.spawn.sandbox.arg.mounts.is_empty()
			&& args.options.spawn.sandbox.arg.ports.is_empty()
			&& args.options.spawn.sandbox.arg.network.get().is_none()
			&& matches!(args.options.spawn.stdin, Some(tg::process::Stdio::Null))
			&& matches!(args.options.spawn.stdout, Some(tg::process::Stdio::Log))
			&& matches!(args.options.spawn.stderr, Some(tg::process::Stdio::Log))
			&& !matches!(
				args.options.spawn.tty.tty.as_ref(),
				Some(tg::Either::Left(true) | tg::Either::Right(_))
			);
		let cacheable = cacheable || args.options.spawn.checksum.is_some();
		if !cacheable {
			return Err(tg::error!("a build must be cacheable"));
		}

		if args.options.view.is_none() {
			args.options.view = Some(crate::process::run::View::Inline);
		}

		let output = self.run(args).await?;

		Ok(output)
	}
}
