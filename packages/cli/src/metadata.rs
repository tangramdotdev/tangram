use {crate::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;

		// Get the reference.
		let referent = self.get_resolved_reference(&args.reference).await?;
		let is_process = matches!(
			referent.item(),
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map(|item| match item {
				tg::get::Item::Id(id) => id.try_into(),
				tg::get::Item::Pointer(_) => unreachable!(),
			})?;
			let args = crate::process::metadata::Args {
				locations,
				print,
				process,
			};
			self.command_process_metadata(args).await?;
		} else {
			let object = referent.into_graph_edge()?.try_map(|edge| {
				edge.try_unwrap_object()
					.map(|object| object.id())
					.map_err(|_| tg::error!("expected an object"))
			})?;
			let args = crate::object::metadata::Args {
				locations,
				object,
				print,
			};
			self.command_object_metadata(args).await?;
		}

		Ok(())
	}
}
