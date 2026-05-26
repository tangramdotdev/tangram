use {
	crate::{DeleteArg, Grant, GrantArg, Object, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	dashmap::DashMap,
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
	grant_ttl: u64,
	grants: DashMap<(tg::object::Id, tg::Principal), Grant, fnv::FnvBuildHasher>,
	objects: DashMap<tg::object::Id, Object<'static>, tg::id::BuildHasher>,
}

impl Store {
	#[must_use]
	pub fn new(config: &Config) -> Self {
		Self {
			grant_ttl: config.grant_ttl,
			grants: DashMap::default(),
			objects: DashMap::default(),
		}
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::new(&Config { grant_ttl: 86_400 })
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		Ok(Store::try_get_sync(self, &arg))
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		Ok(Store::try_get_batch_sync(self, &arg))
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		Store::put(self, arg);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		Store::put_batch(self, args);
		Ok(())
	}

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		Store::grant(self, arg);
		Ok(())
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		Store::grant_batch(self, args);
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		Store::delete(self, arg);
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		Store::delete_batch(self, args);
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		Store::flush(self);
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
			principal: principal.clone(),
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
			principal,
		});
		assert!(output.object.is_none());
		assert!(output.grants.is_empty());
	}
}
