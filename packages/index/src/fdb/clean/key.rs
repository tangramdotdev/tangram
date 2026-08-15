use tangram_client::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum ItemKind {
	AccountObject = 4,
	AccountProcess = 5,
	Checkout = 0,
	Object = 1,
	Process = 2,
	Sandbox = 3,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Key {
	AccountObject {
		account: crate::usage::Account,
		object: tg::object::Id,
		partition: u64,
		touched_at: i64,
	},
	AccountProcess {
		account: crate::usage::Account,
		partition: u64,
		process: tg::process::Id,
		touched_at: i64,
	},
	Checkout {
		id: tg::artifact::Id,
		partition: u64,
		touched_at: i64,
	},
	Object {
		id: tg::object::Id,
		partition: u64,
		touched_at: i64,
	},
	Process {
		id: tg::process::Id,
		partition: u64,
		touched_at: i64,
	},
	Sandbox {
		id: tg::sandbox::Id,
		partition: u64,
		touched_at: i64,
	},
}

#[cfg(test)]
mod tests {
	use {
		super::Key,
		crate::fdb::{Index, Kind},
		foundationdb_tuple as fdbt,
		num_traits::ToPrimitive as _,
		tangram_client::prelude::*,
	};

	#[test]
	fn roundtrips_centralized_clean_keys() {
		let subspace = fdbt::Subspace::all();
		let account = crate::usage::Account::User(tg::user::Id::new());
		let artifact = tg::artifact::Id::new(tg::artifact::Kind::Directory, &vec![0].into());
		let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![1].into());
		let process = tg::process::Id::new();
		let sandbox = tg::sandbox::Id::new();
		let keys = [
			Key::AccountObject {
				account: account.clone(),
				object: object.clone(),
				partition: 1,
				touched_at: 2,
			},
			Key::AccountProcess {
				account,
				partition: 1,
				process: process.clone(),
				touched_at: 2,
			},
			Key::Checkout {
				id: artifact,
				partition: 1,
				touched_at: 2,
			},
			Key::Object {
				id: object,
				partition: 1,
				touched_at: 2,
			},
			Key::Process {
				id: process,
				partition: 1,
				touched_at: 2,
			},
			Key::Sandbox {
				id: sandbox,
				partition: 1,
				touched_at: 2,
			},
		];
		let prefix = Index::pack(&subspace, &(Kind::Clean.to_i32().unwrap(),));
		for expected in keys {
			let key = crate::fdb::Key::Clean(expected.clone());
			let bytes = Index::pack(&subspace, &key);
			assert!(bytes.starts_with(&prefix));
			let crate::fdb::Key::Clean(actual) = Index::unpack(&subspace, &bytes).unwrap() else {
				panic!("expected a clean key");
			};
			assert_eq!(actual, expected);
		}
	}
}
