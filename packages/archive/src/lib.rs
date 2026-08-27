pub mod object;
pub mod s3;

pub trait Archive {
	fn delete_object(
		&self,
		arg: object::delete::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn put_object(
		&self,
		arg: object::put::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<()>> + Send;

	fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> impl std::future::Future<Output = tangram_client::Result<object::get::Output>> + Send;
}
