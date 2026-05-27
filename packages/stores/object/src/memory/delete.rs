use {super::Store, crate::DeleteArg, num::ToPrimitive as _};

impl Store {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		let mut state = self.state();
		let remove = state
			.objects
			.get(&arg.id)
			.is_some_and(|entry| entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap());
		let removed = remove.then(|| state.objects.remove(&arg.id)).flatten();
		if removed.is_some() {
			let keys = state
				.grants
				.keys()
				.filter(|(id, _)| id == &arg.id)
				.cloned()
				.collect::<Vec<_>>();
			for key in keys {
				state.remove_grant(&key);
			}
		}
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		let mut state = self.state();
		for arg in args {
			let remove = state
				.objects
				.get(&arg.id)
				.is_some_and(|entry| entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap());
			let removed = remove.then(|| state.objects.remove(&arg.id)).flatten();
			if removed.is_some() {
				let keys = state
					.grants
					.keys()
					.filter(|(id, _)| id == &arg.id)
					.cloned()
					.collect::<Vec<_>>();
				for key in keys {
					state.remove_grant(&key);
				}
			}
		}
	}
}
