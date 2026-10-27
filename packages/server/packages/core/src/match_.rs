use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The match component.
pub trait Component: crate::component::Lifecycle {
	fn match_<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::match_::Arg,
	) -> BoxFuture<'a, tg::Result<tg::match_::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
