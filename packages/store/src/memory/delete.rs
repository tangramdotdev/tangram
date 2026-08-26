use {super::Store, crate::object, num::ToPrimitive as _};

impl Store {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_object(&self, arg: object::delete::Arg) {
		let mut state = self.state();
		let remove = state
			.objects
			.get(&arg.id)
			.is_some_and(|entry| entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap());
		if remove {
			state.objects.remove(&arg.id);
		}
	}

	pub fn delete_object_batch(&self, args: Vec<object::delete::Arg>) {
		let mut state = self.state();
		for arg in args {
			let remove = state
				.objects
				.get(&arg.id)
				.is_some_and(|entry| entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap());
			if remove {
				state.objects.remove(&arg.id);
			}
		}
	}
}
