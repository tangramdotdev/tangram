use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub use crate::spawn::{Args, Options};

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let Args {
		options,
		reference,
		trailing,
		..
	} = args;
	let options: Options = options;
	let output = crate::spawn::spawn_inner(cli, options, reference, trailing).await?;

	if output.tag.specifier.is_some() {
		return Err(tg::error!("a tag is not supported for an exec"));
	}

	tg::Process::<tg::Value>::exec_with_instance(&output.client, output.arg).await
}
