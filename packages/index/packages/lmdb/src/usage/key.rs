use tangram_client::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Key {
	AccountObject {
		account: tangram_index::usage::Account,
		object: tg::object::Id,
	},
	AccountProcess {
		account: tangram_index::usage::Account,
		process: tg::process::Id,
	},
	Aggregate {
		account: tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
	},
	Aggregation {
		account: tangram_index::usage::Account,
		hour: i64,
		partition: u64,
	},
	Delta {
		account: tangram_index::usage::Account,
		hour: i64,
		kind: tangram_index::usage::DeltaKind,
		partition: u64,
	},
	ObjectAccount {
		account: tangram_index::usage::Account,
		object: tg::object::Id,
	},
	ProcessAccount {
		account: tangram_index::usage::Account,
		process: tg::process::Id,
	},
	Started,
	Unavailable {
		account: tangram_index::usage::Account,
		kind: tangram_index::usage::PeriodKind,
		partition: u64,
	},
}
