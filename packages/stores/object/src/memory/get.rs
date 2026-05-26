use {
	super::Store,
	crate::{Grant, Object, TryGetArg, TryGetBatchArg, TryGetOutput},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	#[must_use]
	pub fn try_get_sync(&self, arg: &TryGetArg) -> TryGetOutput {
		let object = self.try_get_object(&arg.id);
		let grants = self.try_get_grant(&arg.id, &arg.principal, arg.now);
		TryGetOutput { grants, object }
	}

	#[must_use]
	pub fn try_get_batch_sync(&self, arg: &TryGetBatchArg) -> Vec<TryGetOutput> {
		arg.ids
			.iter()
			.map(|id| TryGetOutput {
				grants: self.try_get_grant(id, &arg.principal, arg.now),
				object: self.try_get_object(id),
			})
			.collect()
	}

	pub fn try_get_data(&self, id: &tg::object::Id) -> tg::Result<Option<(u64, tg::object::Data)>> {
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

	#[must_use]
	fn try_get_object(&self, id: &tg::object::Id) -> Option<Object<'static>> {
		self.objects.get(id).map(|entry| entry.clone())
	}

	pub(super) fn try_get_grant(
		&self,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> Vec<Grant> {
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
}
