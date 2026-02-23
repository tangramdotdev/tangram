use {
	crate::Cli,
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

pub mod create;
pub mod delete;
pub mod exec;
pub mod run;
pub mod serve;

/// Manage sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Create(self::create::Args),
	Delete(self::delete::Args),
	Exec(self::exec::Args),
	Run(self::run::Args),
	Serve(self::serve::Args),
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
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

	#[clap(flatten)]
	pub network: Network,

	/// The desired user.
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

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Network {
	/// Whether to allow network access
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_network",
		require_equals = true,
	)]
	network: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "network",
		require_equals = true,
	)]
	no_network: Option<bool>,
}

impl Network {
	pub fn get(&self) -> bool {
		self.network.or(self.no_network.map(|v| !v)).unwrap_or(true)
	}
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_sync(args: Args) -> std::process::ExitCode {
		match args.command {
			Command::Run(args) => Cli::command_sandbox_run(args),
			Command::Serve(args) => Cli::command_sandbox_serve(args),
			_ => unreachable!(),
		}
	}

	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => {
				self.command_sandbox_create(args).await?;
			},
			Command::Delete(args) => {
				self.command_sandbox_delete(args).await?;
			},
			Command::Exec(args) => {
				self.command_sandbox_exec(args).await?;
			},
			Command::Run(_) | Command::Serve(_) => {
				unreachable!()
			},
		}
		Ok(())
	}
}

pub(crate) fn parse_env(arg: &str) -> Result<(String, String), String> {
	let (name, value) = arg
		.split_once('=')
		.ok_or_else(|| "expected NAME=value".to_owned())?;
	Ok((name.to_owned(), value.to_owned()))
}

pub(crate) fn parse_mount(arg: &str) -> Result<Mount, String> {
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
					return Err("type already specified".to_owned());
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
