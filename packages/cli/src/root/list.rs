use crate::Cli;
use tangram_client as tg;

/// List roots.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_root_list(&self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::root::list::Arg::default();
		let roots = client.list_roots(arg).await?;
		for root in roots.data {
			println!("{} {}", root.name, root.item);
		}
		Ok(())
	}
}
