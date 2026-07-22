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
	outbox: BTreeMap<(u64, u128), bytes::Bytes>,
	outbox_id: u128,
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

	async fn delete_outbox(&self, arg: crate::outbox::DeleteArg) -> tg::Result<()> {
		self.delete_outbox(arg);
		Ok(())
	}

	async fn dequeue_outbox(
		&self,
		arg: crate::outbox::DequeueArg,
	) -> tg::Result<Vec<crate::outbox::Item>> {
		self.dequeue_outbox(arg)
	}

	async fn enqueue_outbox(&self, arg: crate::outbox::EnqueueArg) -> tg::Result<()> {
		self.enqueue_outbox(arg)
	}

	async fn try_get_outbox_id_at_or_before(
		&self,
		arg: crate::outbox::TryGetIdArg,
	) -> tg::Result<Option<crate::outbox::Id>> {
		self.try_get_outbox_id_at_or_before(arg)
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::borrow::Cow};

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
