use {super::Store, crate::DeleteArg, num::ToPrimitive as _};

impl Store {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		let removed = self.objects.remove_if(&arg.id, |_, entry| {
			entry.stored_at <= arg.now - arg.ttl.to_i64().unwrap()
		});
		if removed.is_some() {
			self.grants.retain(|(id, _), _| id != &arg.id);
		}
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}
}
