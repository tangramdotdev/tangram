use {
	std::{
		io::Write as _,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::{fs::OpenOptionsExt as _, process::CommandExt as _},
		},
		path::Path,
		process::ExitCode,
	},
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

pub struct Config {
	reader: OwnedFd,
	task: std::thread::JoinHandle<std::io::Result<()>>,
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

pub fn configure_command(command: &mut tokio::process::Command, arg: &Arg) -> tg::Result<Config> {
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;
	let mut descriptors = [-1; 2];
	// SAFETY: The array has space for both descriptors returned by the pipe syscall.
	#[cfg(target_os = "linux")]
	let result = unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) };
	// SAFETY: The array has space for both descriptors returned by the pipe syscall.
	#[cfg(not(target_os = "linux"))]
	let result = unsafe { libc::pipe(descriptors.as_mut_ptr()) };
	if result != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to create the sandbox configuration pipe"
		));
	}
	// SAFETY: A successful pipe call returns two newly owned descriptors.
	let (reader, writer) = unsafe {
		(
			OwnedFd::from_raw_fd(descriptors[0]),
			OwnedFd::from_raw_fd(descriptors[1]),
		)
	};
	for fd in [&reader, &writer] {
		// SAFETY: The owned descriptors remain valid while their flags are set.
		if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to protect the sandbox configuration descriptor"
			));
		}
	}
	let fd = reader.as_raw_fd();
	command
		.arg("sandbox")
		.arg("run")
		.arg("--config-fd")
		.arg(fd.to_string());
	// SAFETY: The closure only invokes an async-signal-safe syscall, and Config retains the descriptor until spawn completes.
	unsafe {
		command.as_std_mut().pre_exec(move || {
			if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
	let task = std::thread::Builder::new()
		.spawn(move || {
			let mut writer = std::fs::File::from(writer);
			writer.write_all(&contents)
		})
		.map_err(|error| tg::error!(!error, "failed to start the sandbox configuration writer"))?;
	let config = Config { reader, task };
	Ok(config)
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

impl Config {
	pub fn finish(self) -> tg::Result<()> {
		drop(self.reader);
		self.task
			.join()
			.map_err(|_| tg::error!("the sandbox configuration writer panicked"))?
			.map_err(|error| {
				tg::error!(!error, "failed to write the sandbox launch configuration")
			})?;
		Ok(())
	}
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
	use super::*;

	#[test]
	fn configuration_round_trip() {
		for backend in [
			Backend::Builtin(linux::Arg::default()),
			Backend::Bubblewrap {
				arg: linux::Arg::default(),
				path: "bwrap".into(),
			},
			Backend::Gvisor {
				arg: linux::Arg::default(),
				path: "runsc".into(),
				platform: crate::Platform::Kvm,
			},
		] {
			let arg = Arg { backend };
			let value = serde_json::to_value(&arg).unwrap();
			let decoded: Arg = serde_json::from_value(value.clone()).unwrap();
			assert_eq!(serde_json::to_value(decoded).unwrap(), value);
		}
	}

	#[tokio::test]
	async fn large_configuration_pipe() {
		let mut launch = linux::Arg::default();
		for index in 0..64 {
			launch
				.process
				.environment
				.insert(format!("KEY_{index}"), "x".repeat(4096));
		}
		let arg = Arg {
			backend: Backend::Builtin(launch),
		};
		let contents = serde_json::to_vec(&arg).unwrap();
		let mut command = tokio::process::Command::new("/bin/sh");
		command
			.arg("-c")
			.arg("for arg; do fd=$arg; done; eval \"wc -c <&$fd\"")
			.arg("transport-test");
		command.stdout(std::process::Stdio::piped());
		let config = configure_command(&mut command, &arg).unwrap();
		let child = command.spawn().unwrap();
		config.finish().unwrap();
		let output = child.wait_with_output().await.unwrap();
		assert!(output.status.success());
		assert_eq!(
			String::from_utf8(output.stdout)
				.unwrap()
				.trim()
				.parse::<usize>()
				.unwrap(),
			contents.len()
		);
	}
}
