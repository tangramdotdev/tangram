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
		position: i64,
		process: tg::process::Id,
		sandbox: tg::sandbox::Id,
	},
	SandboxRunner {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
}
