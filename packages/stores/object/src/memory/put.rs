use {
	super::Store,
	crate::{Object, PutArg},
	std::borrow::Cow,
};

impl Store {
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
}
