use {
	super::Store,
	crate::{Object, TryGetArg, TryGetBatchArg, TryGetOutput},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	#[must_use]
	pub fn try_get_sync(&self, arg: &TryGetArg) -> TryGetOutput {
		let state = self.state();
		let object = Self::try_get_object(&state, &arg.id);
		TryGetOutput { object }
	}

	#[must_use]
	pub fn try_get_batch_sync(&self, arg: &TryGetBatchArg) -> Vec<TryGetOutput> {
		let state = self.state();
		arg.ids
			.iter()
			.map(|id| TryGetOutput {
				object: Self::try_get_object(&state, id),
			})
			.collect()
	}

	pub fn try_get_data(&self, id: &tg::object::Id) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let state = self.state();
		let Some(entry) = state.objects.get(id) else {
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
	fn try_get_object(state: &super::State, id: &tg::object::Id) -> Option<Object<'static>> {
		state.objects.get(id).cloned()
	}
}
