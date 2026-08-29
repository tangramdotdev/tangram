use {super::Store, crate::object, tangram_client::prelude::*};

impl Store {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let timestamp = object::cache::stored_at_timestamp(arg.touched_at)?;
		let remove = state
			.objects
			.get(&arg.id)
			.is_some_and(|object| object.timestamp <= timestamp);
		if remove {
			state.objects.remove(&arg.id);
		}

		Ok(())
	}

	pub fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		let mut state = self.state();
		for arg in args {
			let timestamp = object::cache::stored_at_timestamp(arg.touched_at)?;
			let remove = state
				.objects
				.get(&arg.id)
				.is_some_and(|object| object.timestamp <= timestamp);
			if remove {
				state.objects.remove(&arg.id);
			}
		}

		Ok(())
	}
}
