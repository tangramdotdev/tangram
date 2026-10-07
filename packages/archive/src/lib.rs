use tangram_client::prelude::*;

pub mod object;

pub trait Archive {
	fn delete_object(
		&self,
		arg: object::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_object(&self, arg: object::put::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> impl Future<Output = tg::Result<object::get::Output>> + Send;
}
