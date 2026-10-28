use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The module component.
pub trait Component: crate::component::Lifecycle {
	fn resolve_module<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'a, tg::Result<tg::module::resolve::Output>>;

	fn load_module<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'a, tg::Result<tg::module::load::Output>>;

	fn module_interface<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::module::interface::Arg,
	) -> BoxFuture<'a, tg::Result<tg::module::interface::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
