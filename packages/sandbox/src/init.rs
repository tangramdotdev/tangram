use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Arg {
	#[cfg(target_os = "linux")]
	Builtin(crate::builtin::init::Arg),

	#[cfg(target_os = "linux")]
	Bubblewrap(crate::bubblewrap::init::Arg),

	#[cfg(target_os = "linux")]
	CloudHypervisor(crate::cloud_hypervisor::init::Arg),

	#[cfg(target_os = "linux")]
	Gvisor(crate::gvisor::init::Arg),

	#[cfg(target_os = "macos")]
	Seatbelt(crate::seatbelt::init::Arg),
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	match arg {
		#[cfg(target_os = "linux")]
		Arg::Builtin(arg) => crate::builtin::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::Bubblewrap(arg) => crate::bubblewrap::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::CloudHypervisor(arg) => crate::cloud_hypervisor::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::Gvisor(arg) => crate::gvisor::init::run(arg),

		#[cfg(target_os = "macos")]
		Arg::Seatbelt(arg) => crate::seatbelt::init::run(arg),
	}
}

pub(crate) fn command(arg: &crate::serve::Arg) -> std::process::Command {
	let mut command = std::process::Command::new(&arg.tangram_path);
	command
		.arg("sandbox")
		.arg("serve")
		.arg(if arg.listen { "--listen" } else { "--connect" })
		.arg("--output-path")
		.arg(&arg.output_path)
		.arg("--url")
		.arg(arg.url.to_string())
		.arg("--tangram-path")
		.arg(&arg.tangram_path)
		.args(crate::util::http2_args(&arg.http2));
	for path in &arg.library_paths {
		command.arg("--library-path").arg(path);
	}
	command
}
