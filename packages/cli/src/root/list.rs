use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// List roots.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_root_list(&self, _args: Args) -> tg::Result<()> {
		let arg = tg::root::list::Arg::default();
		let roots = self.handle.list_roots(arg).await?;
		for root in roots.items {
			println!("{} {}", root.name, root.id);
		}
		Ok(())
	}
}
