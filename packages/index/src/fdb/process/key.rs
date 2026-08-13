use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Process(tg::process::Id),
	ProcessChild {
		child: tg::process::Id,
		position: i64,
		process: tg::process::Id,
	},
	ChildProcess {
		child: tg::process::Id,
		parent: tg::process::Id,
	},
	ProcessObject {
		process: tg::process::Id,
		kind: crate::process::object::Kind,
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
}
