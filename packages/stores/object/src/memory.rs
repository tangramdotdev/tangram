use {
	crate::{DeleteArg, Grant, GrantArg, Object, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	std::{
		collections::{BTreeMap, HashMap, HashSet},
		sync::{Arc, Mutex, MutexGuard},
	},
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod grant;
mod put;

#[derive(Clone, Debug)]
pub struct Config {
	pub grant_ttl: u64,
}

pub struct Store {
	#[expect(dead_code)]
	grant_clean_task: Option<tangram_futures::task::Task<()>>,
	grant_ttl: u64,
	state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
	grants: Grants,
	grants_by_created_at: BTreeMap<i64, GrantKeys>,
	objects: Objects,
}

type GrantKey = (tg::object::Id, tg::Principal);
type GrantKeys = HashSet<GrantKey, fnv::FnvBuildHasher>;
type Grants = HashMap<GrantKey, Grant, fnv::FnvBuildHasher>;
type Objects = HashMap<tg::object::Id, Object<'static>, tg::id::BuildHasher>;

impl Store {
	#[must_use]
	pub fn new(config: &Config) -> Self {
		let state = Arc::new(Mutex::new(State::default()));
		let grant_clean_task = tokio::runtime::Handle::try_current()
			.ok()
			.map(|_| Self::spawn_grant_clean_task(&state, config.grant_ttl));
		Self {
			grant_clean_task,
			grant_ttl: config.grant_ttl,
			state,
		}
	}

	fn state(&self) -> MutexGuard<'_, State> {
		self.state
			.lock()
			.expect("failed to lock the memory store state")
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::new(&Config { grant_ttl: 86_400 })
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		Ok(self.try_get_sync(&arg))
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		Ok(self.try_get_batch_sync(&arg))
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		self.put_batch(args);
		Ok(())
	}

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		self.grant(arg);
		Ok(())
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		self.grant_batch(args);
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg);
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args);
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::borrow::Cow};

	#[test]
	fn delete_removes_object_grants() {
		let store = Store::default();
		let principal = tg::Principal::User(tg::user::Id::new());
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		store.put(crate::PutArg {
			bytes: Some(bytes.clone()),
			cache_pointer: None,
			id: id.clone(),
			principal: Some(principal.clone()),
			stored_at: 10,
		});

		let output = store.try_get_sync(&crate::TryGetArg {
			id: id.clone(),
			now: 11,
			principal: Some(principal.clone()),
		});
		assert_eq!(
			output.object.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
		assert!(!output.grants.is_empty());

		store.delete(crate::DeleteArg {
			id: id.clone(),
			now: 16,
			ttl: 5,
		});

		let output = store.try_get_sync(&crate::TryGetArg {
			id,
			now: 17,
			principal: Some(principal),
		});
		assert!(output.object.is_none());
		assert!(output.grants.is_empty());
	}

	#[test]
	fn clean_grants_removes_expired_grants() {
		let store = Store::new(&Config { grant_ttl: 5 });
		let expired_bytes = Bytes::from_static(b"expired");
		let live_bytes = Bytes::from_static(b"live");
		let updated_bytes = Bytes::from_static(b"updated");
		let expired_id = tg::object::Id::new(tg::object::Kind::Blob, &expired_bytes);
		let live_id = tg::object::Id::new(tg::object::Kind::Blob, &live_bytes);
		let updated_id = tg::object::Id::new(tg::object::Kind::Blob, &updated_bytes);
		let principal = tg::Principal::User(tg::user::Id::new());

		store.grant(crate::GrantArg {
			created_at: 10,
			id: expired_id.clone(),
			principal: principal.clone(),
			subtree: false,
		});
		store.grant(crate::GrantArg {
			created_at: 11,
			id: live_id.clone(),
			principal: principal.clone(),
			subtree: false,
		});
		store.grant(crate::GrantArg {
			created_at: 10,
			id: updated_id.clone(),
			principal: principal.clone(),
			subtree: false,
		});
		store.grant(crate::GrantArg {
			created_at: 11,
			id: updated_id.clone(),
			principal: principal.clone(),
			subtree: true,
		});

		Store::clean_grants(&mut store.state(), 15, store.grant_ttl);

		let state = store.state();
		assert!(!state.grants.contains_key(&(expired_id, principal.clone())));
		assert!(state.grants.contains_key(&(live_id, principal.clone())));
		let updated_grant = state.grants.get(&(updated_id, principal)).unwrap();
		assert!(updated_grant.subtree);
	}
}
