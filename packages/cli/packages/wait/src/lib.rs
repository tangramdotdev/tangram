use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Wait for a process to finish or a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	/// Select the source of process or sandbox state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::process::Source,
}

pub async fn run(cli: &mut Cli, mut args: Args) -> tg::Result<()> {
	args.locations.set_from_reference_if_unset(&args.reference);
	let reference = args.locations.apply_to_reference(&args.reference);
	let locations = args.locations;
	let referent = tangram_cli_get::get_with_follow(cli, &reference).await?;
	let id = match referent.node {
		tg::get::Node::Id(id) => id,
		tg::get::Node::Pointer(_) => {
			return Err(tg::error!("expected a process or sandbox"));
		},
	};
	match id.kind() {
		tg::id::Kind::Process => {
			let process = tg::Referent::new(id.try_into()?, referent.options);
			let options = tangram_cli_process::wait::Options {
				locations,
				print: args.print,
				source: args.source,
			};
			tangram_cli_process::wait::run_inner(cli, process, options).await?;
		},
		tg::id::Kind::Sandbox => {
			let sandbox = tg::Referent::new(id.try_into()?, referent.options);
			let sandbox = tg::Sandbox::with_referent(sandbox);
			let options = tangram_cli_sandbox::wait::Options {
				locations,
				print: args.print,
				source: args.source,
			};
			tangram_cli_sandbox::wait::run_inner(cli, sandbox, options).await?;
		},
		_ => return Err(tg::error!(%id, "expected a process or sandbox")),
	}
	Ok(())
}
