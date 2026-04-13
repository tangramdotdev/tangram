use {crate::vm::Network, std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub gid: libc::gid_t,
	pub hostname: Option<String>,
	pub network: Option<Network>,
	pub serve: crate::serve::Arg,
	pub uid: libc::uid_t,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	let _ = arg;
	Err(tg::error!("vm isolation is not yet implemented"))
}
