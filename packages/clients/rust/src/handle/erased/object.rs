use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Object: Send + Sync + 'static {
	fn try_get_object_metadata<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>>;

	fn try_get_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn put_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_object_batch(&self, arg: tg::object::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn touch_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;
}

impl<T> Object for T
where
	T: tg::handle::Object,
{
	fn try_get_object_metadata<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id, arg).boxed()
	}

	fn try_get_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id, arg).boxed()
	}

	fn put_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object(id, arg).boxed()
	}

	fn post_object_batch(&self, arg: tg::object::batch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_object_batch(arg).boxed()
	}

	fn touch_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.touch_object(id, arg).boxed()
	}
}
