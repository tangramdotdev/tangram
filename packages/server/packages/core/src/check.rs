use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The check component.
pub trait Component: crate::component::Lifecycle {
	fn check<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::check::Arg,
	) -> BoxFuture<'a, tg::Result<tg::check::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
