use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The format component.
pub trait Component: crate::component::Lifecycle {
	fn format<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::format::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
