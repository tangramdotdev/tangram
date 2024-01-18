use crate::{Cli, API_URL};
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::{Addr, Handle};
use url::Url;

/// Manage the server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	/// Get the server's health.
	Health,

	/// Start the server.
	Start,

	/// Stop the server.
	Stop,

	/// Run the server.
	Run(RunArgs),
}

/// Run a server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
#[allow(clippy::struct_excessive_bools)]
pub struct RunArgs {
	/// The address to bind to.
	#[arg(long)]
	pub address: Option<Addr>,

	/// The path where Tangram should store its data. The default is `$HOME/.tangram`.
	#[arg(long)]
	pub path: Option<PathBuf>,

	/// Run without a remote.
	#[arg(long, default_value = "false")]
	pub no_remote: bool,

	/// The URL of the remote server.
	#[arg(long)]
	pub remote: Option<Url>,

	/// The Builder settings.
	#[command(flatten)]
	pub builder: Option<BuilderArgs>,
}

#[derive(Debug, clap::Args)]
pub struct BuilderArgs {
	/// Enable the builder.
	#[arg(long, default_value = "false")]
	enable_builder: Option<bool>,

	/// The host the builder should run builds for.
	#[arg(long)]
	hosts: Option<Vec<tg::System>>,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_server(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Health => {
				let addr = tg::Addr::Unix(self.path.join("socket"));
				let client = tg::Builder::new(addr).build();
				let health = client.health().await?;
				let health = serde_json::to_string_pretty(&health).unwrap();
				println!("{health}");
			},
			Command::Start => {
				self.start_server().await?;
			},
			Command::Stop => {
				let addr = tg::Addr::Unix(self.path.join("socket"));
				let client = tg::Builder::new(addr).build();
				client.stop().await?;
			},
			Command::Run(args) => {
				self.command_server_run(args).await?;
			},
		}
		Ok(())
	}

	async fn command_server_run(&self, args: RunArgs) -> Result<()> {
		// Get the path.
		let path = if let Some(path) = args.path.clone() {
			path
		} else {
			self.path.clone()
		};

		// Get the addr.
		let addr = args.address.unwrap_or(Addr::Unix(path.join("socket")));

		// Get the config.
		let config = self.config().await?;

		// Get the user.
		let user = self.user().await?;

		// Create the remote options.
		let url = args
			.remote
			.or(config
				.as_ref()
				.and_then(|config| config.remote.as_ref())
				.and_then(|remote| remote.url.clone()))
			.unwrap_or_else(|| API_URL.parse().unwrap());
		let tls = url.scheme() == "https";
		let client = tg::Builder::new(url.try_into()?)
			.tls(tls)
			.user(user)
			.build();
		let build = config
			.as_ref()
			.and_then(|config| config.remote.as_ref())
			.and_then(|remote| remote.build.clone())
			.map(|build| tangram_server::RemoteBuildOptions {
				enable: build.enable,
				hosts: build.hosts,
			});
		let remote = tangram_server::RemoteOptions {
			tg: Box::new(client),
			build,
		};
		let remote = if args.no_remote { None } else { Some(remote) };

		// Create the build options.
		let max_concurrency = config
			.as_ref()
			.and_then(|config| config.build.as_ref())
			.and_then(|build| build.max_concurrency);
		let build = tangram_server::BuildOptions { max_concurrency };

		let version = self.version.clone();

		let vfs = self
			.config()
			.await?
			.and_then(|config| config.vfs)
			.map(|cfg| tangram_server::VfsOptions {
				enable: cfg.enable.unwrap_or(true),
			});

		// Create the options.
		let options = tangram_server::Options {
			addr,
			build,
			path,
			remote,
			version,
			vfs,
		};

		// Start the server.
		let server = tangram_server::Server::start(options)
			.await
			.wrap_err("Failed to create the server.")?;

		// Wait for the server to stop or stop it with an interrupt signal.
		tokio::spawn({
			let server = server.clone();
			async move {
				tokio::signal::ctrl_c().await.ok();
				server.stop();
				tokio::signal::ctrl_c().await.ok();
				std::process::exit(130);
			}
		});

		server.join().await.wrap_err("Failed to join the server.")?;

		Ok(())
	}
}
