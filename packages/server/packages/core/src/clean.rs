use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The clean component.
pub trait Component: crate::component::Lifecycle {
	fn clean<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::clean::Output>>>>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
