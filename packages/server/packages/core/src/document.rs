use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The document component.
pub trait Component: crate::component::Lifecycle {
	fn document<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::document::Arg,
	) -> BoxFuture<'a, tg::Result<tg::document::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
