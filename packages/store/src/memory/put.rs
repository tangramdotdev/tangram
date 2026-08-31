use {super::Store, crate::object, std::borrow::Cow, tangram_client::prelude::*};

impl Store {
	pub fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let mut state = self.state();
		if state
			.objects
			.get(&arg.id)
			.is_some_and(|object| object.object.put > arg.put)
		{
			return Ok(());
		}
		let object = object::Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: arg.checkout_pointer,
			length: arg.length,
			put: arg.put,
		};
		let object = super::Object { object };
		state.objects.insert(arg.id.clone(), object);

		Ok(())
	}

	pub fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		let mut state = self.state();
		for arg in args {
			if state
				.objects
				.get(&arg.id)
				.is_some_and(|object| object.object.put > arg.put)
			{
				continue;
			}
			let object = object::Object {
				bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
				checkout_pointer: arg.checkout_pointer,
				length: arg.length,
				put: arg.put,
			};
			let object = super::Object { object };
			state.objects.insert(arg.id.clone(), object);
		}

		Ok(())
	}
}
