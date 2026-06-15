use {
	super::Store,
	crate::{Object, PutArg},
	std::borrow::Cow,
};

impl Store {
	pub fn put(&self, arg: PutArg) {
		let mut state = self.state();
		let object = Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			cache_pointer: arg.cache_pointer,
			stored_at: arg.stored_at,
		};
		state.objects.insert(arg.id.clone(), object);
	}

	pub fn put_batch(&self, args: Vec<PutArg>) {
		let mut state = self.state();
		for arg in args {
			let object = Object {
				bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
				cache_pointer: arg.cache_pointer,
				stored_at: arg.stored_at,
			};
			state.objects.insert(arg.id.clone(), object);
		}
	}
}
