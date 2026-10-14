use {
	std::{path::PathBuf, process::ExitCode},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	#[serde(flatten)]
	pub linux: crate::linux::run::Arg,

	pub path: PathBuf,
}

pub fn run(arg: Arg) -> tg::Result<ExitCode> {
	crate::linux::run::run(arg.linux, &crate::Backend::Bubblewrap { path: arg.path })
}
