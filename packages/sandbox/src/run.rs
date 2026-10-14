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

pub use crate::command::Command;

#[cfg(target_os = "linux")]
pub type Bind = crate::mount::Bind;
#[cfg(target_os = "linux")]
pub type Mounts = crate::mount::Arg;
#[cfg(target_os = "linux")]
pub type Overlay = crate::mount::Overlay;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	pub backend: Backend,

	pub command: Command,

	#[serde(default)]
	pub options: Options,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
pub enum Backend {
	#[cfg(target_os = "linux")]
	Builtin(crate::builtin::run::Arg),

	#[cfg(target_os = "linux")]
	Bubblewrap(crate::bubblewrap::run::Arg),

	#[cfg(target_os = "linux")]
	CloudHypervisor(crate::cloud_hypervisor::run::Arg),

	#[cfg(target_os = "linux")]
	Gvisor(crate::gvisor::run::Arg),

	#[cfg(target_os = "macos")]
	Seatbelt(crate::seatbelt::run::Arg),
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
	#[cfg(target_os = "linux")]
	pub cgroup: Option<Cgroup>,

	#[cfg(target_os = "linux")]
	pub cgroup_readonly: bool,

	#[cfg(target_os = "linux")]
	pub filesystem_fd: Option<i32>,

	#[cfg(target_os = "linux")]
	pub filesystem_path: Option<std::path::PathBuf>,

	#[cfg(target_os = "linux")]
	pub fuse_fd: Option<i32>,

	#[cfg(target_os = "linux")]
	pub fuse_path: Option<std::path::PathBuf>,

	#[cfg(target_os = "linux")]
	pub index: u64,

	#[cfg(target_os = "linux")]
	pub isolate_root: bool,

	#[cfg(target_os = "linux")]
	pub mounts: Mounts,

	#[cfg(target_os = "linux")]
	pub network: Option<Network>,

	#[cfg(target_os = "linux")]
	pub nice: u8,

	#[cfg(target_os = "linux")]
	pub rlimit_nofile: Option<u64>,

	#[cfg(target_os = "linux")]
	pub user_namespace_fd: Option<i32>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cgroup {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
	pub memory_swap: Option<u64>,
	pub name: String,
	pub pids: Option<u64>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Network {
	Host,
	Pasta {
		fd: i32,
	},
	Veth {
		fd: i32,
		gateway: std::net::Ipv4Addr,
		guest: std::net::Ipv4Addr,
	},
}

pub struct Config {
	reader: OwnedFd,
	task: std::thread::JoinHandle<std::io::Result<()>>,
}

pub fn run(arg: Arg) -> tg::Result<ExitCode> {
	// Validate the command before preparing the sandbox.
	if arg.command.args.is_empty() {
		return Err(tg::error!("a command is required"));
	}

	#[cfg(target_os = "linux")]
	{
		let mut options = arg.options;
		let prepared = crate::prepare::prepare(&mut options, &arg.backend)?;
		let output = crate::prepare::run(&arg.command, &options, arg.backend, prepared)?;

		Ok(output)
	}

	#[cfg(target_os = "macos")]
	{
		let Backend::Seatbelt(backend) = arg.backend;
		let output = crate::seatbelt::run::run(&arg.command, &backend)?;

		Ok(ExitCode::from(output))
	}
}

pub fn configure_command(command: &mut tokio::process::Command, arg: &Arg) -> tg::Result<Config> {
	// Serialize the configuration.
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;

	// Create the pipe.
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

	// Own the pipe descriptors.
	// SAFETY: A successful pipe call returns two newly owned descriptors.
	let (reader, writer) = unsafe {
		(
			OwnedFd::from_raw_fd(descriptors[0]),
			OwnedFd::from_raw_fd(descriptors[1]),
		)
	};

	// Close the descriptors when executing other commands.
	for fd in [&reader, &writer] {
		// SAFETY: The owned descriptors remain valid while their flags are set.
		if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to protect the sandbox configuration descriptor"
			));
		}
	}

	// Set the launch arguments.
	let fd = reader.as_raw_fd();
	command
		.arg("sandbox")
		.arg("run")
		.arg("--config-fd")
		.arg(fd.to_string());

	// Inherit the reader in the sandbox command.
	// SAFETY: The closure only invokes an async-signal-safe syscall, and Config retains the descriptor until spawn completes.
	unsafe {
		command.as_std_mut().pre_exec(move || {
			if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}

	// Start the configuration writer.
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
	// Serialize the configuration.
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;

	// Create the file.
	let mut file = std::fs::OpenOptions::new()
		.create_new(true)
		.mode(0o600)
		.write(true)
		.open(path)
		.map_err(|error| {
			tg::error!(!error, path = %path.display(), "failed to create the sandbox launch configuration")
		})?;

	// Write the configuration.
	file.write_all(&contents).map_err(|error| {
		tg::error!(!error, path = %path.display(), "failed to write the sandbox launch configuration")
	})?;

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
			Backend::Builtin(crate::builtin::run::Arg::default()),
			Backend::Bubblewrap(crate::bubblewrap::run::Arg {
				path: "bwrap".into(),
				..Default::default()
			}),
			Backend::Gvisor(crate::gvisor::run::Arg {
				path: "runsc".into(),
				platform: crate::Platform::Kvm,
				..Default::default()
			}),
		] {
			let arg = Arg {
				backend,
				command: Command::default(),
				options: Options::default(),
			};
			let value = serde_json::to_value(&arg).unwrap();
			let decoded: Arg = serde_json::from_value(value.clone()).unwrap();
			assert_eq!(serde_json::to_value(decoded).unwrap(), value);
		}
	}

	#[tokio::test]
	async fn large_configuration_pipe() {
		// Create a configuration larger than the pipe buffer.
		let mut command_arg = Command::default();
		for index in 0..64 {
			command_arg
				.env
				.insert(format!("KEY_{index}"), "x".repeat(4096));
		}
		let arg = Arg {
			backend: Backend::Builtin(crate::builtin::run::Arg::default()),
			command: command_arg,
			options: Options::default(),
		};
		let contents = serde_json::to_vec(&arg).unwrap();

		// Create a command that counts the configuration bytes.
		let mut command = tokio::process::Command::new("/bin/sh");
		command
			.arg("-c")
			.arg("for arg; do fd=$arg; done; eval \"wc -c <&$fd\"")
			.arg("transport-test");
		command.stdout(std::process::Stdio::piped());

		// Spawn the command and finish writing the configuration.
		let config = configure_command(&mut command, &arg).unwrap();
		let child = command.spawn().unwrap();
		config.finish().unwrap();

		// Verify that the command received the complete configuration.
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
