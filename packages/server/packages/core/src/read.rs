use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub use tangram_server_read_types::{File, Loader, Object, ReadFuture, Reader};

/// The read component.
pub trait Component: crate::component::Lifecycle {
	fn reader<'a>(
		&'a self,
		session: &'a Session,
		blob: tg::Blob,
	) -> BoxFuture<'a, tg::Result<Reader>>;

	fn reader_sync(&self, session: &Session, blob: tg::Blob) -> tg::Result<Reader>;

	fn try_read_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::read::Event>>>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
