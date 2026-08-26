use {super::Store, crate::object, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	#[must_use]
	pub fn try_get_object_sync(&self, arg: &object::get::Arg) -> object::get::Output {
		let state = self.state();
		let object = Self::try_get_object_inner(&state, &arg.id);
		object::get::Output { object }
	}

	#[must_use]
	pub fn try_get_object_batch_sync(
		&self,
		arg: &object::get::batch::Arg,
	) -> Vec<object::get::Output> {
		let state = self.state();
		arg.ids
			.iter()
			.map(|id| object::get::Output {
				object: Self::try_get_object_inner(&state, id),
			})
			.collect()
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
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
	fn try_get_object_inner(
		state: &super::State,
		id: &tg::object::Id,
	) -> Option<object::Object<'static>> {
		state.objects.get(id).cloned()
	}
}
