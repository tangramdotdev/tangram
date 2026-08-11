use {crate::usage, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	AccountObject {
		account: usage::Account,
		object: tg::object::Id,
	},
	AccountProcess {
		account: usage::Account,
		process: tg::process::Id,
	},
	AccountUsage {
		account: usage::Account,
		kind: usage::Kind,
		partition: u64,
	},
	ObjectAccount {
		account: usage::Account,
		object: tg::object::Id,
	},
	ProcessAccount {
		account: usage::Account,
		process: tg::process::Id,
	},
}
