use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Process(tg::process::Id),
	ProcessChild {
		child: tg::process::Id,
		position: i64,
		process: tg::process::Id,
	},
	ProcessData {
		position: i64,
		process: tg::process::Id,
	},
	ChildProcess {
		child: tg::process::Id,
		parent: tg::process::Id,
	},
	ProcessObject {
		process: tg::process::Id,
		kind: tangram_index::process::object::Kind,
		object: tg::object::Id,
	},
	CommandCacheableProcess {
		command: tg::object::Id,
		process: tg::process::Id,
	},
	ProcessSandbox {
		process: tg::process::Id,
		sandbox: tg::sandbox::Id,
	},
	CreatorProcess {
		creator: tg::Principal,
		process: tg::process::Id,
		status: tg::process::Status,
	},
	OwnerProcess {
		owner: tg::Principal,
		process: tg::process::Id,
		status: tg::process::Status,
	},
}

impl Key {
	#[must_use]
	pub fn principals(
		creator: &tg::Principal,
		owner: &tg::Principal,
		status: tg::process::Status,
		process: &tg::process::Id,
	) -> [Self; 2] {
		let creator = Self::CreatorProcess {
			creator: creator.clone(),
			process: process.clone(),
			status,
		};
		let owner = Self::OwnerProcess {
			owner: owner.clone(),
			process: process.clone(),
			status,
		};
		[creator, owner]
	}
}
