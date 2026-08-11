use {crate::storage, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	AccountObject {
		account: storage::Account,
		object: tg::object::Id,
	},
	AccountProcess {
		account: storage::Account,
		process: tg::process::Id,
	},
	AccountUsage {
		account: storage::Account,
		kind: storage::Kind,
		partition: u64,
	},
	ObjectAccount {
		account: storage::Account,
		object: tg::object::Id,
	},
	ProcessAccount {
		account: storage::Account,
		process: tg::process::Id,
	},
}
