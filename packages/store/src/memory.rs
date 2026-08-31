use {
	crate::object,
	std::{
		collections::{BTreeMap, HashMap},
		sync::{Arc, Mutex, MutexGuard},
	},
	tangram_client::prelude::*,
};

mod archive;
mod cache;
mod delete;
mod flush;
mod get;
mod log;
mod outbox;
mod put;

#[derive(Clone, Debug, Default)]
pub struct Config {}

pub struct Store {
	state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct Log {
	entries: BTreeMap<u64, crate::log::read::Entry<'static>>,
	stream_positions: BTreeMap<(tg::process::stdio::Stream, u64), u64>,
}

#[derive(Default)]
struct State {
	logs: Logs,
	object_archive_outbox: BTreeMap<(u64, [u8; 16]), tg::object::Id>,
	object_cache: BTreeMap<(u64, [u8; 16]), (tg::object::Id, [u8; 16])>,
	object_index_outbox: BTreeMap<(u64, [u8; 16], u64), bytes::Bytes>,
	objects: Objects,
}

#[derive(Clone)]
struct Object {
	object: object::Object<'static>,
}

type Logs = HashMap<tg::process::Id, Log, tg::id::BuildHasher>;
type Objects = HashMap<tg::object::Id, Object, tg::id::BuildHasher>;

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
	async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		let arg = object::get::Arg {
			id: arg.id,
			put: Some(arg.put),
		};
		let output = self.try_get_object_sync(&arg);

		Ok(output.object.is_some())
	}

	async fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) -> tg::Result<()> {
		self.delete_object_cache_entry(arg);
		Ok(())
	}

	async fn delete_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_archive_outbox_entries(arg);
		Ok(())
	}

	async fn delete_log(&self, arg: crate::log::delete::Arg) -> tg::Result<()> {
		self.delete_log(arg);
		Ok(())
	}

	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg)
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		self.delete_object_batch(args)
	}

	async fn delete_object_index_outbox_batch(
		&self,
		arg: crate::object::index::outbox::batch::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_index_outbox_batch(arg);
		Ok(())
	}

	async fn delete_object_index_outbox_fragments(
		&self,
		arg: crate::object::index::outbox::fragment::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_index_outbox_fragments(arg);
		Ok(())
	}

	async fn dequeue_object_index_outbox_fragments(
		&self,
		arg: crate::object::index::outbox::fragment::dequeue::Arg,
	) -> tg::Result<Vec<crate::object::index::outbox::fragment::Fragment>> {
		self.dequeue_object_index_outbox_fragments(arg)
	}

	async fn dequeue_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::dequeue::Arg,
	) -> tg::Result<Vec<object::archive::outbox::Entry>> {
		self.dequeue_object_archive_outbox_entries(arg)
	}

	async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		Ok(self.get_object_cache_entries(arg))
	}

	async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		self.put_object_cache_entry(arg)?;
		Ok(())
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg)?;
		Ok(())
	}

	async fn put_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::put::Arg,
	) -> tg::Result<()> {
		self.put_object_archive_outbox_entries(arg);
		Ok(())
	}

	async fn enqueue_object_index_outbox_batch(
		&self,
		arg: crate::object::index::outbox::batch::enqueue::Arg,
	) -> tg::Result<()> {
		self.enqueue_object_index_outbox_batch(arg)
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}

	async fn put_log(&self, arg: crate::log::put::Arg) -> tg::Result<()> {
		self.put_log(arg);
		Ok(())
	}

	async fn put_log_batch(&self, args: Vec<crate::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args);
		Ok(())
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		self.put_object(arg)
	}

	async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args)
	}

	async fn try_get_log_length(&self, arg: crate::log::length::Arg) -> tg::Result<Option<u64>> {
		Ok(self.try_get_log_length(&arg))
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		Ok(self.try_get_object_sync(&arg))
	}

	async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		Ok(self.try_get_object_batch_sync(&arg))
	}

	async fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: crate::object::index::outbox::batch::get::Arg,
	) -> tg::Result<Option<crate::object::index::outbox::batch::Id>> {
		self.try_get_object_index_outbox_batch_at_or_before(arg)
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<crate::capacity::Capacity>> {
		Ok(None)
	}

	async fn try_read_log(
		&self,
		arg: crate::log::read::Arg,
	) -> tg::Result<Vec<crate::log::read::Entry<'static>>> {
		Ok(self.try_read_log(arg))
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
		let checkout_pointer = object::checkout::Pointer {
			artifact: tg::file::Id::new(b"first").into(),
			length: 5,
			path: Some(PathBuf::from("first")),
			position: 1,
		};
		store
			.put_object(object::put::Arg {
				bytes: Some(first_bytes),
				checkout_pointer: Some(checkout_pointer),
				id: id.clone(),
				length: Some(5),
				put: [1; 16],
			})
			.unwrap();

		let second_bytes = Bytes::from_static(b"second");
		store
			.put_object_batch(vec![object::put::Arg {
				bytes: Some(second_bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				put: [2; 16],
			}])
			.unwrap();

		let object = store
			.try_get_object_sync(&object::get::Arg { id, put: None })
			.object
			.unwrap();
		assert_eq!(object.bytes, Some(Cow::Owned(second_bytes.to_vec())));
		assert!(object.checkout_pointer.is_none());
		assert!(object.length.is_none());
		assert_eq!(object.put, [2; 16]);
	}

	// An exact get returns only the requested put.
	#[tokio::test]
	async fn get_exact_put() {
		let store = Store::default();
		let bytes = Bytes::from_static(b"bytes");
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		store
			.put_object(object::put::Arg {
				bytes: Some(bytes),
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				put: [2; 16],
			})
			.unwrap();

		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: Some([1; 16]),
		});
		assert!(output.object.is_none());
		let contains = crate::Store::contains_object(
			&store,
			object::contains::Arg {
				id: id.clone(),
				put: [1; 16],
			},
		)
		.await
		.unwrap();
		assert!(!contains);
		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: Some([2; 16]),
		});
		assert_eq!(output.object.unwrap().put, [2; 16]);
		let contains =
			crate::Store::contains_object(&store, object::contains::Arg { id, put: [2; 16] })
				.await
				.unwrap();
		assert!(contains);
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

		store
			.put_object(object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				put: [10; 16],
			})
			.unwrap();
		store
			.put_object(object::put::Arg {
				bytes: Some(Bytes::from_static(b"stale")),
				checkout_pointer: None,
				id: id.clone(),
				length: None,
				put: [9; 16],
			})
			.unwrap();

		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: None,
		});
		let object = output.object.unwrap();
		assert_eq!(object.bytes, Some(Cow::Owned(bytes.to_vec())));
		assert_eq!(object.put, [10; 16]);

		store
			.delete_object(object::delete::Arg {
				id: id.clone(),
				put: [9; 16],
			})
			.unwrap();
		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: None,
		});
		assert!(output.object.is_some());

		store
			.delete_object(object::delete::Arg {
				id: id.clone(),
				put: [10; 16],
			})
			.unwrap();

		let output = store.try_get_object_sync(&object::get::Arg { id, put: None });
		assert!(output.object.is_none());
	}
}
