use {
	crate::{Session, temp::Temp},
	futures::future::BoxFuture,
	std::pin::Pin,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tokio::io::AsyncRead,
};

pub use tangram_server_checkin_core::write::Output;

/// The write component.
pub trait Component: crate::component::Lifecycle {
	fn write<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::write::Arg,
		reader: Pin<Box<dyn AsyncRead + Send + 'static>>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>>;

	fn write_inner_sync<'a>(
		&'a self,
		session: &'a Session,
		reader: &'a mut dyn std::io::Read,
		destination: Option<&'a Destination>,
		progress: &'a tg::progress::Handle<crate::checkin::TaskOutput>,
	) -> tg::Result<Output>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}

pub enum Destination {
	Cache,
	Temp(Temp),
}
