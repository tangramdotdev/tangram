use {
	crate::Result,
	std::process::ExitCode,
	tangram_tests_util::{Instance, instance},
};

/// Create instances and server configurations for the e2e tests.
#[derive(clap::Subcommand)]
pub enum Subcommand {
	/// Print the configuration of a server in an instance, with the server's own configuration merged.
	Config {
		/// The server's own configuration as JSON.
		#[arg(long)]
		config: Option<String>,

		/// The instance's description as JSON.
		#[arg(long)]
		instance: String,

		#[arg(long)]
		region: Option<String>,
	},

	/// Create an instance and print its description as JSON.
	Create {
		#[arg(long)]
		cloud: bool,

		/// The configuration that every server in the instance merges, as JSON.
		#[arg(long)]
		config: Option<String>,

		#[arg(long)]
		primary_region: Option<String>,

		#[arg(long = "region")]
		regions: Vec<String>,

		#[arg(long)]
		semaphore_namespace: Option<String>,
	},
}

pub async fn command(subcommand: Subcommand) -> Result<ExitCode> {
	match subcommand {
		Subcommand::Config {
			config: server,
			instance,
			region,
		} => {
			let instance = Instance::with_data(serde_json::from_str(&instance)?);
			let mut config = instance.config(region.as_deref())?;
			if let Some(server) = server {
				config.merge(serde_json::from_str(&server)?);
			}
			let config = instance.server_config(config)?;
			println!("{}", serde_json::to_string(config.value())?);
		},
		Subcommand::Create {
			cloud,
			config,
			primary_region,
			regions,
			semaphore_namespace,
		} => {
			let config = config
				.map(|config| serde_json::from_str(&config))
				.transpose()?
				.unwrap_or_default();
			let arg = instance::Arg {
				config,
				primary_region,
				regions,
				semaphore_namespace,
			};
			let instance = if cloud {
				Instance::cloud(arg)
					.await?
					.ok_or("the cloud databases are not available")?
			} else {
				Instance::local(arg)?
			};
			println!("{}", serde_json::to_string(&instance.into_data())?);
		},
	}
	Ok(ExitCode::SUCCESS)
}
