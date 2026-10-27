use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The health component.
pub trait Component: crate::component::Lifecycle {
	fn health<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::health::Arg,
	) -> BoxFuture<'a, tg::Result<tg::Health>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
