use {
	crate::Cli,
	std::{net::Ipv4Addr, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(action = clap::ArgAction::Append, long = "dns", num_args = 1)]
	pub dns_servers: Vec<Ipv4Addr>,

	#[arg(long)]
	pub gateway_ip: Option<Ipv4Addr>,

	#[arg(long)]
	pub gid: u32,

	#[arg(long)]
	pub guest_ip: Option<Ipv4Addr>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long)]
	pub netmask: Option<Ipv4Addr>,

	#[arg(long)]
	pub network: bool,

	#[arg(long)]
	pub output_path: PathBuf,

	#[arg(long)]
	pub tangram_path: PathBuf,

	#[arg(long)]
	pub uid: u32,

	#[arg(long)]
	pub url: tangram_uri::Uri,
}

impl Args {
	fn into_arg(self) -> tg::Result<tangram_sandbox::vm::init::Arg> {
		let network = if self.network {
			if self.dns_servers.is_empty() {
				return Err(tg::error!("missing a DNS server for vm networking"));
			}
			Some(tangram_sandbox::vm::Network {
				dns_servers: self.dns_servers,
				gateway_ip: self
					.gateway_ip
					.ok_or_else(|| tg::error!("missing the gateway IP for vm networking"))?,
				guest_ip: self
					.guest_ip
					.ok_or_else(|| tg::error!("missing the guest IP for vm networking"))?,
				netmask: self
					.netmask
					.ok_or_else(|| tg::error!("missing the netmask for vm networking"))?,
			})
		} else {
			None
		};
		Ok(tangram_sandbox::vm::init::Arg {
			gid: self.gid,
			hostname: self.hostname,
			network,
			serve: tangram_sandbox::serve::Arg {
				library_paths: Vec::new(),
				listen: false,
				output_path: self.output_path,
				tangram_path: self.tangram_path,
				url: self.url,
			},
			uid: self.uid,
		})
	}
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_vm_init(args: Args) -> std::process::ExitCode {
		let arg = match args.into_arg() {
			Ok(arg) => arg,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				return std::process::ExitCode::FAILURE;
			},
		};
		let result = tangram_sandbox::vm::init::run(&arg);
		match result {
			Ok(code) => code,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
