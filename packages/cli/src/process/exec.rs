use {crate::Cli, tangram_client::prelude::*};

pub use crate::process::spawn::Args;

pub type Options = crate::process::spawn::Options;

impl Cli {
	pub async fn command_process_exec(&mut self, args: Args) -> tg::Result<()> {
		let Args {
			options,
			reference,
			trailing,
			..
		} = args;
		let options: Options = options;
		let output = self.spawn_inner(options, reference, trailing).await?;

		if output.tag.is_some() {
			return Err(tg::error!("a tag is not supported for an exec"));
		}

		tg::Process::<tg::Value>::exec_with_handle(&output.client, output.arg).await
	}
}
