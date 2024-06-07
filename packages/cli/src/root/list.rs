use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// List roots.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_root_list(&self, _args: Args) -> tg::Result<()> {
		let arg = tg::root::list::Arg::default();
		let roots = self.handle.list_roots(arg).await?;
		for root in roots.data {
			println!("{} {}", root.name, root.item);
		}
		Ok(())
	}
}
