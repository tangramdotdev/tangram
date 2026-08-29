use {super::Store, crate::object, std::borrow::Cow, tangram_client::prelude::*};

impl Store {
	pub fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let timestamp = object::cache::stored_at_timestamp(arg.stored_at)?;
		if state
			.objects
			.get(&arg.id)
			.is_some_and(|object| object.timestamp > timestamp)
		{
			return Ok(());
		}
		let object = object::Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: arg.checkout_pointer,
			length: arg.length,
			stored_at: arg.stored_at,
		};
		let object = super::Object { object, timestamp };
		state.objects.insert(arg.id.clone(), object);

		Ok(())
	}

	pub fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		let mut state = self.state();
		for arg in args {
			let timestamp = object::cache::stored_at_timestamp(arg.stored_at)?;
			if state
				.objects
				.get(&arg.id)
				.is_some_and(|object| object.timestamp > timestamp)
			{
				continue;
			}
			let object = object::Object {
				bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
				checkout_pointer: arg.checkout_pointer,
				length: arg.length,
				stored_at: arg.stored_at,
			};
			let object = super::Object { object, timestamp };
			state.objects.insert(arg.id.clone(), object);
		}

		Ok(())
	}
}
