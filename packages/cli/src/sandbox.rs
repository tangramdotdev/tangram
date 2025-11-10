use {
	crate::Cli,
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Debug, Clone, clap::Args)]
pub struct Args {
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

impl Cli {
	#[must_use]
	pub fn command_sandbox(args: Args) -> std::process::ExitCode {
		// Create the command.
		let command = tangram_sandbox::Command {
			chroot: args.chroot,
			cwd: args.cwd,
			env: args.env,
			executable: args.executable,
			hostname: args.hostname,
			mounts: args
				.mounts
				.into_iter()
				.map(|mount| tangram_sandbox::Mount {
					source: mount.source,
					target: mount.target,
					fstype: mount.fstype,
					flags: mount.flags,
					data: mount.data,
				})
				.collect(),
			network: args.network,
			trailing: args.trailing,
			user: args.user,
		};

		// Run the sandbox.
		#[cfg(target_os = "linux")]
		let result = tangram_sandbox::linux::spawn(command);
		#[cfg(target_os = "macos")]
		let result = tangram_sandbox::darwin::spawn(command);

		match result {
			Ok(status) => status,
			Err(error) => {
				let error = tg::error!(!error, "failed to run the sandbox");
				let error = tg::Referent::with_item(error);
				Self::print_error_basic(error);
				std::process::ExitCode::FAILURE
			},
		}
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
					use num::ToPrimitive as _;
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
