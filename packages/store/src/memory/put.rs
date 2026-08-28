use {super::Store, crate::object, std::borrow::Cow};

impl Store {
	pub fn put_object(&self, arg: object::put::Arg) {
		let mut state = self.state();
		if state
			.objects
			.get(&arg.id)
			.is_some_and(|object| object.stored_at > arg.stored_at)
		{
			return;
		}
		let object = object::Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: arg.checkout_pointer,
			length: arg.length,
			stored_at: arg.stored_at,
		};
		state.objects.insert(arg.id.clone(), object);
	}

	pub fn put_object_batch(&self, args: Vec<object::put::Arg>) {
		let mut state = self.state();
		for arg in args {
			if state
				.objects
				.get(&arg.id)
				.is_some_and(|object| object.stored_at > arg.stored_at)
			{
				continue;
			}
			let object = object::Object {
				bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
				checkout_pointer: arg.checkout_pointer,
				length: arg.length,
				stored_at: arg.stored_at,
			};
			state.objects.insert(arg.id.clone(), object);
		}
	}
}
