use {crate::storage, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	ObjectOwner {
		object: tg::object::Id,
		owner: storage::Owner,
	},
	OwnerObject {
		object: tg::object::Id,
		owner: storage::Owner,
	},
	OwnerObjectClean {
		object: tg::object::Id,
		owner: storage::Owner,
		partition: u64,
		touched_at: i64,
	},
	OwnerProcess {
		owner: storage::Owner,
		process: tg::process::Id,
	},
	OwnerProcessClean {
		owner: storage::Owner,
		partition: u64,
		process: tg::process::Id,
		touched_at: i64,
	},
	OwnerStorage {
		kind: storage::Kind,
		owner: storage::Owner,
		partition: u64,
	},
	ProcessOwner {
		owner: storage::Owner,
		process: tg::process::Id,
	},
}
