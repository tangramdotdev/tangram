use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	CreatorSandbox {
		creator: tg::Principal,
		sandbox: tg::sandbox::Id,
	},
	OwnerSandbox {
		owner: tg::Principal,
		sandbox: tg::sandbox::Id,
	},
	Sandbox(tg::sandbox::Id),
	SandboxProcess {
		sandbox: tg::sandbox::Id,
		process: tg::process::Id,
	},
	SandboxRunner {
		sandbox: tg::sandbox::Id,
		runner: tg::runner::Id,
	},
}
