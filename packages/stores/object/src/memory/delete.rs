use {super::Store, crate::DeleteArg, num::ToPrimitive as _};

impl Store {
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
}
