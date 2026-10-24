use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The tag component.
pub trait Component: crate::component::Lifecycle {
	fn delete_tags<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'a, tg::Result<tg::tag::delete::Output>>;

	fn put_tag<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_tag_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_tag<'a>(
		&'a self,
		session: &'a Session,
		tag: &'a tg::tag::Selector,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn pull_tag<'a>(
		&'a self,
		session: &'a Session,
		pattern: tg::specifier::Pattern,
		location: Option<tg::location::Arg>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_tag_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::tag::Id,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn try_get_tag_remote<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::tag::Id,
		arg: tg::tag::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn try_get_tag_data_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::tag::Id,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::tag::Data>, crate::database::Error>>>;

	fn get_tag_data_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::tag::Id,
	) -> BoxFuture<'a, tg::Result<ControlFlow<tg::tag::Data, crate::database::Error>>>;
}
