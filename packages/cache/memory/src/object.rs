use {
	crate::Cache, num::ToPrimitive as _, std::borrow::Cow, tangram_cache::object,
	tangram_client::prelude::*,
};

impl Cache {
	#[must_use]
	pub fn contains_object(&self, arg: object::contains::Arg) -> bool {
		let arg = object::get::Arg {
			bytes: false,
			id: arg.id,
			put: Some(arg.put),
		};
		let output = self.try_get_object_sync(&arg);

		output.object.is_some()
	}

	pub fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) {
		let mut state = self.state();
		let entry = arg.entry;
		if state
			.objects
			.get(&entry.id)
			.is_some_and(|object| object.object.put == entry.put)
		{
			state.objects.remove(&entry.id);
		}
		state.object_cache.remove(&(entry.partition, entry.cache));
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let remove = state
			.objects
			.get(&arg.id)
			.is_some_and(|object| object.object.put == arg.put);
		if remove {
			state.objects.remove(&arg.id);
		}

		Ok(())
	}

	pub fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		let mut state = self.state();
		for arg in args {
			let remove = state
				.objects
				.get(&arg.id)
				.is_some_and(|object| object.object.put == arg.put);
			if remove {
				state.objects.remove(&arg.id);
			}
		}

		Ok(())
	}

	#[must_use]
	pub fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> Vec<object::cache::Entry> {
		let state = self.state();
		state
			.object_cache
			.iter()
			.filter(|((partition, _), _)| *partition == arg.partition)
			.take(arg.batch_size)
			.map(|((partition, cache), (id, put))| object::cache::Entry {
				cache: *cache,
				id: id.clone(),
				partition: *partition,
				put: *put,
			})
			.collect()
	}

	pub fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		self.state()
			.object_cache
			.insert((arg.partition, arg.cache), (arg.id, arg.put));

		Ok(())
	}

	pub fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let object = arg.object;
		let mut state = self.state();
		state
			.object_cache
			.insert((arg.partition, arg.cache), (object.id.clone(), object.put));
		let previous = state.objects.get(&object.id);
		if previous.is_some_and(|previous| previous.object.put > object.put) {
			return Ok(());
		}
		let value = object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};
		let value = crate::Object { object: value };
		state.objects.insert(object.id, value);

		Ok(())
	}

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
		let object = crate::Object { object };
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
			let object = crate::Object { object };
			state.objects.insert(arg.id.clone(), object);
		}

		Ok(())
	}

	#[must_use]
	pub fn try_get_object_sync(&self, arg: &object::get::Arg) -> object::get::Output {
		let state = self.state();
		let object = Self::try_get_object_inner(&state, &arg.id, arg.bytes);
		let object = object.filter(|object| arg.put.is_none_or(|put| object.put == put));
		object::get::Output { object }
	}

	#[must_use]
	fn try_get_object_inner(
		state: &crate::State,
		id: &tg::object::Id,
		bytes: bool,
	) -> Option<object::Object<'static>> {
		state.objects.get(id).map(|entry| object::Object {
			bytes: bytes.then(|| entry.object.bytes.clone()).flatten(),
			checkout_pointer: entry.object.checkout_pointer.clone(),
			length: entry.object.length,
			put: entry.object.put,
		})
	}

	#[must_use]
	pub fn try_get_object_batch_sync(
		&self,
		arg: &object::get::batch::Arg,
	) -> Vec<object::get::Output> {
		let state = self.state();
		arg.ids
			.iter()
			.map(|id| object::get::Output {
				object: Self::try_get_object_inner(&state, id, arg.bytes),
			})
			.collect()
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let state = self.state();
		let Some(entry) = state.objects.get(id) else {
			return Ok(None);
		};
		let Some(bytes) = &entry.object.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(id.kind(), bytes.as_ref())?;
		Ok(Some((size, data)))
	}
}

impl tangram_cache::object::Cache for Cache {
	async fn contains_object(&self, arg: tangram_cache::object::contains::Arg) -> tg::Result<bool> {
		Ok(self.contains_object(arg))
	}

	async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg);
		Ok(())
	}

	async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg)
	}

	async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		self.delete_object_batch(args)
	}

	async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		Ok(self.get_object_cache_entries(arg))
	}

	async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry(arg)?;
		Ok(())
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg)?;
		Ok(())
	}

	async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg)
	}

	async fn put_object_batch(&self, args: Vec<tangram_cache::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args)
	}

	async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		Ok(self.try_get_object_sync(&arg))
	}

	async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		Ok(self.try_get_object_batch_sync(&arg))
	}
}
