use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The children component.
pub trait Component: crate::component::Lifecycle {
	fn children<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::children::Arg,
	) -> BoxFuture<'a, tg::Result<tg::children::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
