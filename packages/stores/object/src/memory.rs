use {
	crate::{DeleteArg, Grant, GrantArg, Object, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	dashmap::DashMap,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub grant_ttl: u64,
}

pub struct Store {
	grant_ttl: u64,
	grants: DashMap<(tg::object::Id, String), Grant, tg::id::BuildHasher>,
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

	#[must_use]
	fn object(&self, id: &tg::object::Id) -> Option<Object<'static>> {
		self.objects.get(id).map(|entry| entry.clone())
	}

	#[must_use]
	pub fn try_get_with_grants(&self, arg: &TryGetArg) -> TryGetOutput {
		let object = self.object(&arg.id);
		let grants = self.grants(&arg.id, &arg.principal, arg.now);
		TryGetOutput { grants, object }
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		if !matches!(principal, tg::Principal::Root) && self.grants(id, principal, now).is_empty() {
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
		let object = Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			cache_pointer: arg.cache_pointer,
			stored_at: arg.stored_at,
		};
		self.objects.insert(arg.id.clone(), object);
		if let Some(principal) = &arg.principal {
			self.put_grant(arg.id.clone(), principal, false, arg.stored_at);
		}
	}

	pub fn put_batch(&self, args: Vec<PutArg>) {
		for arg in args {
			self.put(arg);
		}
	}

	pub fn grant(&self, arg: GrantArg) {
		self.put_grant(arg.id, &arg.principal, arg.subtree, arg.created_at);
	}

	pub fn grant_batch(&self, args: Vec<GrantArg>) {
		for arg in args {
			self.grant(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		self.objects.remove_if(&arg.id, |_, entry| {
			entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}

	pub fn flush(&self) {}

	fn grants(&self, id: &tg::object::Id, principal: &tg::Principal, now: i64) -> Vec<Grant> {
		if matches!(principal, tg::Principal::Root) {
			return Vec::new();
		}
		let principal = principal.to_string();
		self.grants
			.get(&(id.clone(), principal))
			.and_then(|grant| {
				(now - grant.created_at < self.grant_ttl.to_i64().unwrap()).then(|| grant.clone())
			})
			.into_iter()
			.collect()
	}

	fn put_grant(
		&self,
		id: tg::object::Id,
		principal: &tg::Principal,
		subtree: bool,
		created_at: i64,
	) {
		let key = (id, principal.to_string());
		self.grants
			.entry(key)
			.and_modify(|grant| {
				grant.created_at = created_at;
				grant.subtree = grant.subtree || subtree;
			})
			.or_insert(Grant {
				created_at,
				subtree,
			});
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::new(&Config { grant_ttl: 86_400 })
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		Ok(self.try_get_with_grants(&arg))
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let outputs = arg
			.ids
			.iter()
			.map(|id| TryGetOutput {
				grants: self.grants(id, &arg.principal, arg.now),
				object: self.object(id),
			})
			.collect();
		Ok(outputs)
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
