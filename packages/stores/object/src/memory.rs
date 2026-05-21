use {
	crate::{DeleteArg, DeleteMembershipArg, Membership, Object, PutArg},
	dashmap::DashMap,
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::HashMap},
	tangram_client::prelude::*,
};

pub struct Store {
	objects: DashMap<tg::object::Id, Object<'static>, tg::id::BuildHasher>,
	memberships:
		DashMap<tg::object::Id, HashMap<Option<tg::Namespace>, Membership>, tg::id::BuildHasher>,
}

impl Store {
	#[must_use]
	pub fn new() -> Self {
		Self {
			objects: DashMap::default(),
			memberships: DashMap::default(),
		}
	}

	#[must_use]
	pub fn try_get(
		&self,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> Option<Object<'static>> {
		if !self.has_membership(id, namespaces, public) {
			return None;
		}
		self.objects.get(id).map(|entry| entry.clone())
	}

	fn has_membership(
		&self,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> bool {
		let Some(memberships) = self.memberships.get(id) else {
			return false;
		};
		if public && memberships.contains_key(&None) {
			return true;
		}
		memberships.keys().any(|membership_namespace| {
			membership_namespace
				.as_ref()
				.is_some_and(|membership_namespace| {
					namespaces
						.iter()
						.any(|namespace| namespace_is_prefix(namespace, membership_namespace))
				})
		})
	}

	#[must_use]
	pub fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
		namespaces: &[tg::Namespace],
		public: bool,
	) -> Vec<Option<Object<'static>>> {
		ids.iter()
			.map(|id| self.try_get(id, namespaces, public))
			.collect()
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		if !self.has_membership(id, namespaces, public) {
			return Ok(None);
		}
		let Some(entry) = self.objects.get(id) else {
			return Ok(None);
		};
		let Some(bytes) = &entry.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(id.kind(), bytes.as_ref())?;
		Ok(Some((size, data)))
	}

	pub fn put(&self, arg: PutArg) {
		let id = arg.id;
		let existing = self.objects.get(&id);
		let bytes = arg
			.bytes
			.map(|bytes| Cow::Owned(bytes.to_vec()))
			.or_else(|| existing.as_ref().and_then(|object| object.bytes.clone()));
		let cache_pointer = arg.cache_pointer.or_else(|| {
			existing
				.as_ref()
				.and_then(|object| object.cache_pointer.clone())
		});
		let object = Object {
			bytes,
			cache_pointer,
			stored_at: arg.stored_at,
		};
		let membership = Membership {
			stored_at: arg.stored_at,
		};
		drop(existing);
		self.objects.insert(id.clone(), object);
		self.memberships
			.entry(id)
			.or_default()
			.insert(arg.namespace, membership);
	}

	pub fn put_batch(&self, args: Vec<PutArg>) {
		for arg in args {
			self.put(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_membership(&self, arg: DeleteMembershipArg) {
		let mut remove_object = false;
		if let Some(mut memberships) = self.memberships.get_mut(&arg.id) {
			let remove_membership = memberships.get(&arg.namespace).is_some_and(|membership| {
				arg.now - membership.stored_at >= arg.ttl.to_i64().unwrap()
			});
			if remove_membership {
				memberships.remove(&arg.namespace);
			}
			remove_object = memberships.is_empty();
		}
		if remove_object {
			self.memberships
				.remove_if(&arg.id, |_, memberships| memberships.is_empty());
		}
	}

	pub fn delete_membership_batch(&self, args: Vec<DeleteMembershipArg>) {
		for arg in args {
			self.delete_membership(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		self.objects.remove_if(&arg.id, |_, entry| {
			arg.now - entry.stored_at >= arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}

	pub fn flush(&self) {}
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Store for Store {
	async fn try_get(
		&self,
		id: &tg::object::Id,
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Option<Object<'static>>> {
		Ok(self.try_get(id, &namespaces, public))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		Ok(self.try_get_batch(ids, &namespaces, public))
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		self.put_batch(args);
		Ok(())
	}

	async fn delete_membership(&self, arg: DeleteMembershipArg) -> tg::Result<()> {
		self.delete_membership(arg);
		Ok(())
	}

	async fn delete_membership_batch(&self, args: Vec<DeleteMembershipArg>) -> tg::Result<()> {
		self.delete_membership_batch(args);
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

fn namespace_is_prefix(prefix: &tg::Namespace, namespace: &tg::Namespace) -> bool {
	let prefix = prefix.components().collect::<Vec<_>>();
	let namespace = namespace.components().collect::<Vec<_>>();
	prefix.len() <= namespace.len()
		&& std::iter::zip(prefix, namespace).all(|(prefix, namespace)| prefix == namespace)
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	fn object(content: &'static [u8]) -> (tg::object::Id, Bytes) {
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		(id, bytes)
	}

	fn namespace(value: &str) -> tg::Namespace {
		value.parse().unwrap()
	}

	#[test]
	fn test_delete_membership_preserves_object() {
		let store = Store::new();
		let (id, bytes) = object(b"hello world");

		store.put(PutArg {
			bytes: Some(bytes.clone()),
			cache_pointer: None,
			id: id.clone(),
			namespace: Some(namespace("foo")),
			stored_at: 100,
		});
		store.put(PutArg {
			bytes: None,
			cache_pointer: None,
			id: id.clone(),
			namespace: Some(namespace("bar")),
			stored_at: 100,
		});

		store.delete_membership(DeleteMembershipArg {
			id: id.clone(),
			namespace: Some(namespace("foo")),
			now: 200,
			ttl: 100,
		});

		assert!(store.try_get(&id, &[namespace("foo")], false).is_none());
		assert!(store.try_get(&id, &[namespace("bar")], false).is_some());
		assert!(
			store
				.try_get_object_data(&id, &[namespace("bar")], false)
				.unwrap()
				.is_some()
		);

		store.delete(DeleteArg {
			id: id.clone(),
			now: 200,
			ttl: 100,
		});

		assert!(store.try_get(&id, &[namespace("bar")], false).is_none());
		assert!(
			store
				.try_get_object_data(&id, &[namespace("bar")], false)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn test_public_and_root_are_distinct() {
		let store = Store::new();
		let (public_id, public_bytes) = object(b"public");
		let (root_id, root_bytes) = object(b"root");

		store.put(PutArg {
			bytes: Some(public_bytes),
			cache_pointer: None,
			id: public_id.clone(),
			namespace: None,
			stored_at: 100,
		});
		store.put(PutArg {
			bytes: Some(root_bytes),
			cache_pointer: None,
			id: root_id.clone(),
			namespace: Some(tg::Namespace::root()),
			stored_at: 100,
		});

		assert!(store.try_get(&public_id, &[], true).is_some());
		assert!(
			store
				.try_get(&public_id, &[tg::Namespace::root()], false)
				.is_none()
		);
		assert!(store.try_get(&root_id, &[], true).is_none());
		assert!(
			store
				.try_get(&root_id, &[tg::Namespace::root()], false)
				.is_some()
		);
		assert!(
			store
				.try_get_object_data(&public_id, &[tg::Namespace::root()], false)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn test_membership_lookup_uses_the_requested_object() {
		let store = Store::new();
		let (foo_id, foo_bytes) = object(b"foo");
		let (bar_id, bar_bytes) = object(b"bar");

		store.put(PutArg {
			bytes: Some(foo_bytes),
			cache_pointer: None,
			id: foo_id.clone(),
			namespace: Some(namespace("foo")),
			stored_at: 100,
		});
		store.put(PutArg {
			bytes: Some(bar_bytes),
			cache_pointer: None,
			id: bar_id.clone(),
			namespace: Some(namespace("bar")),
			stored_at: 100,
		});

		assert!(store.try_get(&foo_id, &[namespace("foo")], false).is_some());
		assert!(store.try_get(&foo_id, &[namespace("bar")], false).is_none());
		assert!(store.try_get(&bar_id, &[namespace("bar")], false).is_some());
		assert!(store.try_get(&bar_id, &[namespace("foo")], false).is_none());
	}
}
