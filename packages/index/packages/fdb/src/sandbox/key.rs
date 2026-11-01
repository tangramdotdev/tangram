use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	CreatorSandbox {
		creator: tg::Principal,
		sandbox: tg::sandbox::Id,
		status: tg::sandbox::Status,
	},
	OwnerSandbox {
		owner: tg::Principal,
		sandbox: tg::sandbox::Id,
		status: tg::sandbox::Status,
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

impl Key {
	#[must_use]
	pub fn principals(
		creator: &tg::Principal,
		owner: &tg::Principal,
		status: tg::sandbox::Status,
		sandbox: &tg::sandbox::Id,
	) -> [Self; 2] {
		let creator = Self::CreatorSandbox {
			creator: creator.clone(),
			sandbox: sandbox.clone(),
			status,
		};
		let owner = Self::OwnerSandbox {
			owner: owner.clone(),
			sandbox: sandbox.clone(),
			status,
		};
		[creator, owner]
	}
}
