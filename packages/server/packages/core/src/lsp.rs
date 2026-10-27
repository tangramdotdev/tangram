use {
	crate::Session,
	futures::future::BoxFuture,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tokio::io::{AsyncBufRead, AsyncWrite},
};

/// The lsp component.
pub trait Component: crate::component::Lifecycle {
	fn lsp<'a>(
		&'a self,
		session: &'a Session,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
