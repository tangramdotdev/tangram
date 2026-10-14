use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	#[serde(flatten)]
	pub linux: crate::linux::run::Arg,
}

pub fn run(arg: Arg) -> tg::Result<ExitCode> {
	crate::linux::run::run(arg.linux, &crate::Backend::Builtin)
}
