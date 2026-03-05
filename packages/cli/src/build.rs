use {crate::Cli, tangram_client::prelude::*};

impl Cli {
	pub async fn command_build(&mut self, mut args: crate::run::Args) -> tg::Result<()> {
		args.options.spawn.sandbox =
			crate::process::spawn::Sandbox::new(args.options.spawn.sandbox.get().or(Some(true)));
		if args.options.spawn.stdin.is_none() {
			args.options.spawn.stdin = Some(tg::run::Stdio::Null);
		}
		if args.options.spawn.stdout.is_none() {
			args.options.spawn.stdout = Some(tg::run::Stdio::Log);
		}
		if args.options.spawn.stderr.is_none() {
			args.options.spawn.stderr = Some(tg::run::Stdio::Log);
		}
		self.command_run(args).await
	}
}
