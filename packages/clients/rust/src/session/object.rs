use crate::prelude::*;

impl tg::handle::Object for tg::Session {
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id, arg)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id, arg)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.put_object(id, arg)
	}

	fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> impl Future<Output = tg::Result<tg::object::batch::Output>> {
		self.post_object_batch(arg)
	}

	fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_touch_object(id, arg)
	}
}
