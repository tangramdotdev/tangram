use {
	std::{io::Write as _, os::unix::fs::OpenOptionsExt as _, path::Path, process::ExitCode},
	tangram_client::prelude::*,
};

#[cfg(target_os = "linux")]
pub mod linux;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	pub backend: Backend,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
pub enum Backend {
	#[cfg(target_os = "linux")]
	Builtin(linux::Arg),
	#[cfg(target_os = "linux")]
	Bubblewrap {
		#[serde(flatten)]
		arg: linux::Arg,
		path: std::path::PathBuf,
	},
	#[cfg(target_os = "linux")]
	CloudHypervisor(crate::cloud_hypervisor::run::Arg),
	#[cfg(target_os = "linux")]
	Gvisor {
		#[serde(flatten)]
		arg: linux::Arg,
		path: std::path::PathBuf,
		platform: crate::Platform,
	},
	#[cfg(target_os = "macos")]
	Seatbelt(crate::seatbelt::run::Arg),
}

pub fn run(arg: Arg) -> tg::Result<ExitCode> {
	match arg.backend {
		#[cfg(target_os = "linux")]
		Backend::Builtin(arg) => linux::run(arg, &crate::Backend::Builtin),
		#[cfg(target_os = "linux")]
		Backend::Bubblewrap { arg, path } => linux::run(arg, &crate::Backend::Bubblewrap { path }),
		#[cfg(target_os = "linux")]
		Backend::CloudHypervisor(arg) => crate::cloud_hypervisor::run::run(&arg),
		#[cfg(target_os = "linux")]
		Backend::Gvisor {
			arg,
			path,
			platform,
		} => linux::run(arg, &crate::Backend::Gvisor { path, platform }),
		#[cfg(target_os = "macos")]
		Backend::Seatbelt(arg) => crate::seatbelt::run::run(&arg),
	}
}

pub fn write_config<T: serde::Serialize>(path: &Path, arg: &T) -> tg::Result<()> {
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;
	let mut file = std::fs::OpenOptions::new().create_new(true).mode(0o600).write(true).open(path)
        .map_err(|error| tg::error!(!error, path = %path.display(), "failed to create the sandbox launch configuration"))?;
	file.write_all(&contents)
        .map_err(|error| tg::error!(!error, path = %path.display(), "failed to write the sandbox launch configuration"))?;
	Ok(())
}
