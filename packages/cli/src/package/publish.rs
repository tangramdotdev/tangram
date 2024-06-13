use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client as tg;

/// Publish a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, default_value = "false")]
	pub locked: bool,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_package_publish(&self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Canonicalize the path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, _) = tg::package::get_with_lock(&client, &args.package, args.locked)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;

		// Get the package ID.
		let id = package.id(&client).await?;

		// Push the package.
		if let Some(remote) = args.remote.clone() {
			let push_args = crate::object::push::Args {
				object: id.clone().into(),
				remote,
			};
			self.command_object_push(push_args).await?;
		}

		// Publish the package.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::package::publish::Arg { remote };
		client
			.publish_package(&id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the package"))?;

		// Print a message.
		let metadata = tg::package::get_metadata(&client, &package).await?;
		println!(
			"{} published {}@{}",
			"success".green().bold(),
			metadata.name.unwrap().red(),
			metadata.version.unwrap().green()
		);

		Ok(())
	}
}
