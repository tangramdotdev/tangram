use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The pull component.
pub trait Component: crate::component::Lifecycle {
	fn pull<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::pull::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::pull::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::pull::Output>>>,
		)>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
