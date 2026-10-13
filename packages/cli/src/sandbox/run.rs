use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
mod linux;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, default_value = "builtin")]
	pub backend: tangram_sandbox::Kind,
	#[arg(long)]
	pub config: Option<PathBuf>,
	#[cfg(target_os = "linux")]
	#[command(flatten)]
	pub linux: self::linux::Args,
}

impl Cli {
	pub fn command_sandbox_run(args: Args) -> tg::Result<std::process::ExitCode> {
		match args.backend {
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Builtin
			| tangram_sandbox::Kind::Bubblewrap
			| tangram_sandbox::Kind::Gvisor => {
				let arg = if args.config.is_some() {
					let mut arg: tangram_sandbox::run::linux::Arg = read_config(args.config)?;
					arg.backend = args.backend;
					arg
				} else {
					args.linux.into_arg(args.backend)
				};
				tangram_sandbox::run::linux::run(arg)
			},
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::CloudHypervisor => {
				let arg = read_config(args.config)?;
				tangram_sandbox::cloud_hypervisor::run::run(&arg)
			},
			#[cfg(target_os = "macos")]
			tangram_sandbox::Kind::Seatbelt => {
				let arg = read_config(args.config)?;
				tangram_sandbox::seatbelt::run::run(&arg)
			},
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Seatbelt => Err(tg::error!("the seatbelt backend requires macos")),
			#[cfg(target_os = "macos")]
			_ => Err(tg::error!("the selected backend requires linux")),
		}
	}
}

fn read_config<T: serde::de::DeserializeOwned>(path: Option<PathBuf>) -> tg::Result<T> {
	let path = path.ok_or_else(|| tg::error!("the backend requires a launch configuration"))?;
	let contents = std::fs::read(&path).map_err(|error| tg::error!(!error, path = %path.display(), "failed to read the sandbox launch configuration"))?;
	let arg = serde_json::from_slice(&contents)
		.map_err(|error| tg::error!(!error, "failed to parse the sandbox launch configuration"))?;
	Ok(arg)
}
