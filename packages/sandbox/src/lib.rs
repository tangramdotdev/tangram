#[cfg(target_os = "macos")]
use num::ToPrimitive as _;
use {
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

#[derive(Debug, Clone, clap::Args)]
#[allow(dead_code)]
pub struct Command {
	/// Provide a path for the chroot.
	#[arg(long)]
	pub chroot: Option<PathBuf>,

	/// Change the working directory prior to spawn.
	#[arg(long, short = 'C')]
	pub cwd: Option<PathBuf>,

	/// Define environment variables.
	#[arg(
		action = clap::ArgAction::Append,
		num_args = 1,
		short = 'e',
		value_parser = parse_env,
	)]
	pub env: Vec<(String, String)>,

	/// The executable path.
	#[arg(index = 1)]
	pub executable: PathBuf,

	/// The desired hostname.
	#[arg(long)]
	pub hostname: Option<String>,

	/// File systems to be mounted.
	#[arg(
		action = clap::ArgAction::Append,
		long = "mount",
		num_args = 1,
		short,
		value_parser = parse_mount,
	)]
	pub mounts: Vec<Mount>,

	/// Whether to enable network access.
	#[arg(long)]
	pub network: bool,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,

	#[arg(long)]
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: Option<PathBuf>,
	pub target: Option<PathBuf>,
	pub fstype: Option<OsString>,
	pub flags: libc::c_ulong,
	pub data: Option<Bytes>,
}

#[must_use]
pub fn main(command: Command) -> std::process::ExitCode {
	#[cfg(target_os = "linux")]
	let result = linux::spawn(command);
	#[cfg(target_os = "macos")]
	let result = darwin::spawn(command);
	match result {
		Ok(status) => status,
		Err(error) => {
			eprintln!("failed to run sandbox command: {error}");
			eprintln!("original invocation:");
			let mut args = std::env::args();
			eprint!("{}", args.next().unwrap());
			for arg in args {
				eprint!(" {arg}");
			}
			eprintln!();
			std::process::ExitCode::FAILURE
		},
	}
}

fn parse_env(arg: &str) -> Result<(String, String), String> {
	let (name, value) = arg
		.split_once('=')
		.ok_or_else(|| "expected NAME=value".to_owned())?;
	Ok((name.to_owned(), value.to_owned()))
}

fn parse_mount(arg: &str) -> Result<Mount, String> {
	let mut source = None;
	let mut target = None;
	let mut fstype = None;
	let mut flags = 0;
	let mut data = Vec::new();
	for opt in arg.split(',') {
		let mut kv = opt.split('=');
		let k = kv
			.next()
			.ok_or_else(|| format!("expected an option {opt}"))?;
		match k {
			"type" => {
				if fstype.is_some() {
					return Err("type already specified".to_string());
				}
				let v = kv.next().ok_or_else(|| format!("expected a value {opt}"))?;
				if v == "bind" {
					#[cfg(target_os = "linux")]
					{
						flags |= libc::MS_BIND | libc::MS_REC;
					}
				} else {
					fstype.replace(v.to_owned().into());
				}
			},
			"source" => {
				if source.is_some() {
					return Err("source already specified".to_owned());
				}
				let v = kv.next().ok_or_else(|| format!("expected a value {opt}"))?;
				source.replace(PathBuf::from(v));
			},
			"target" => {
				if target.is_some() {
					return Err("target already specified".to_owned());
				}
				let v = kv.next().ok_or_else(|| format!("expected a value {opt}"))?;
				target.replace(PathBuf::from(v));
			},
			"ro" => {
				#[cfg(target_os = "macos")]
				{
					flags |= libc::MNT_RDONLY.to_u64().unwrap();
				}

				#[cfg(target_os = "linux")]
				{
					flags |= libc::MS_RDONLY;
				}
			},
			_ => {
				if !data.is_empty() {
					data.push(b',');
				}
				data.extend_from_slice(opt.as_bytes());
			},
		}
	}
	let mount = Mount {
		fstype,
		source,
		target,
		flags,
		data: (!data.is_empty()).then_some(data.into()),
	};
	Ok(mount)
}
