use {
	crate::Cli,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(allow_hyphen_values = true, trailing_var_arg = true)]
	pub arguments: Vec<OsString>,
	#[arg(long, default_value = "builtin", value_parser = crate::sandbox::parse_backend)]
	pub backend: tangram_sandbox::Kind,
	#[arg(long)]
	pub config: Option<PathBuf>,
	#[arg(long)]
	pub runtime_path: Option<PathBuf>,
}

impl Cli {
	pub fn command_sandbox_run(args: Args) -> tg::Result<std::process::ExitCode> {
		match args.backend {
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Builtin => {
				if args.config.is_some() {
					return Err(tg::error!(
						"the builtin backend accepts native bubblewrap arguments"
					));
				}
				let status = tangram_sandbox::builtin::run::run(args.arguments)?;
				Ok(std::process::ExitCode::from(status))
			},
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Bubblewrap | tangram_sandbox::Kind::Gvisor => {
				if args.config.is_some() {
					return Err(tg::error!(
						"the backend accepts its native runtime arguments"
					));
				}
				use std::os::unix::process::CommandExt as _;
				let path = args.runtime_path.unwrap_or_else(|| {
					if args.backend == tangram_sandbox::Kind::Bubblewrap {
						"bwrap".into()
					} else {
						"runsc".into()
					}
				});
				let error = std::process::Command::new(path).args(args.arguments).exec();
				Err(tg::error!(!error, "failed to execute the sandbox backend"))
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
