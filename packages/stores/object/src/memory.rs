use {
	crate::{DeleteArg, Object, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	std::{
		collections::{BTreeMap, HashMap},
		sync::{Arc, Mutex, MutexGuard},
	},
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod outbox;
mod put;

#[derive(Clone, Debug, Default)]
pub struct Config {}

pub struct Store {
	state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
	objects: Objects,
	outbox: BTreeMap<(u64, [u8; 16], u64), bytes::Bytes>,
}

type Objects = HashMap<tg::object::Id, Object<'static>, tg::id::BuildHasher>;

impl Store {
	#[must_use]
	pub fn new() -> Self {
		let state = Arc::new(Mutex::new(State::default()));
		Self { state }
	}

	fn state(&self) -> MutexGuard<'_, State> {
		self.state
			.lock()
			.expect("failed to lock the memory store state")
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
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

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg);
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args);
		Ok(())
	}

	async fn delete_outbox_fragments(&self, arg: crate::outbox::DeleteArg) -> tg::Result<()> {
		self.delete_outbox_fragments(arg);
		Ok(())
	}

	async fn dequeue_outbox_fragments(
		&self,
		arg: crate::outbox::DequeueArg,
	) -> tg::Result<Vec<crate::outbox::Fragment>> {
		self.dequeue_outbox_fragments(arg)
	}

	async fn enqueue_outbox_batch(&self, arg: crate::outbox::Batch) -> tg::Result<()> {
		self.enqueue_outbox_batch(arg)
	}

	async fn try_get_outbox_batch_at_or_before(
		&self,
		arg: crate::outbox::TryGetBatchArg,
	) -> tg::Result<Option<crate::outbox::BatchId>> {
		self.try_get_outbox_batch_at_or_before(arg)
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		bytes::Bytes,
		num::ToPrimitive as _,
		std::{borrow::Cow, path::PathBuf},
	};

	// A put replaces an existing object.
	#[test]
	fn put_replaces_object() {
		let store = Store::default();
		let first_bytes = Bytes::from_static(b"first");
		let id = tg::object::Id::new(tg::object::Kind::Blob, &first_bytes);
		let cache_pointer = crate::CachePointer {
			artifact: tg::file::Id::new(b"first").into(),
			length: 5,
			path: Some(PathBuf::from("first")),
			position: 1,
		};
		store.put(crate::PutArg {
			bytes: Some(first_bytes),
			cache_pointer: Some(cache_pointer),
			id: id.clone(),
			length: Some(5),
			stored_at: 1,
		});

		let second_bytes = Bytes::from_static(b"second");
		store.put_batch(vec![crate::PutArg {
			bytes: Some(second_bytes.clone()),
			cache_pointer: None,
			id: id.clone(),
			length: None,
			stored_at: 2,
		}]);

		let object = store.try_get_sync(&crate::TryGetArg { id }).object.unwrap();
		assert_eq!(object.bytes, Some(Cow::Owned(second_bytes.to_vec())));
		assert!(object.cache_pointer.is_none());
		assert!(object.length.is_none());
		assert_eq!(object.stored_at, 2);
	}

	// Deleting an object removes the object.
	#[test]
	fn delete_removes_object() {
		let store = Store::default();
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
			length: Some(content.len().to_u64().unwrap()),
			stored_at: 10,
		});

		let output = store.try_get_sync(&crate::TryGetArg { id: id.clone() });
		assert_eq!(
			output.object.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);

		store.delete(crate::DeleteArg {
			id: id.clone(),
			now: 16,
			ttl: 5,
		});

		let output = store.try_get_sync(&crate::TryGetArg { id });
		assert!(output.object.is_none());
	}
}
