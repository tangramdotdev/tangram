use tangram_client::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Key {
	AccountObject {
		account: crate::usage::Account,
		object: tg::object::Id,
	},
	AccountProcess {
		account: crate::usage::Account,
		process: tg::process::Id,
	},
	Aggregate {
		account: crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	},
	Compaction {
		account: crate::usage::Account,
		hour: i64,
		partition: u64,
	},
	Delta {
		account: crate::usage::Account,
		hour: i64,
		kind: crate::usage::DeltaKind,
		partition: u64,
	},
	ObjectAccount {
		account: crate::usage::Account,
		object: tg::object::Id,
	},
	ProcessAccount {
		account: crate::usage::Account,
		process: tg::process::Id,
	},
	Started,
	Unavailable {
		account: crate::usage::Account,
		kind: crate::usage::PeriodKind,
		partition: u64,
	},
}
