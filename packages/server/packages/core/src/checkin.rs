use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub type Stream = BoxStream<'static, tg::Result<tg::progress::Event<tg::checkin::Output>>>;

/// The internal options of a checkin, which no request can set.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
	/// Whether the server knows that nothing else can write the files being checked in, so that a destructive checkin can rename them in place without copying them first.
	pub trusted: bool,
}

#[derive(Clone)]
pub struct TaskOutput {
	pub graph: tangram_server_checkin_core::Graph,
}

/// The checkin component.
pub trait Component: crate::component::Lifecycle {
	fn checkin<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkin::Arg,
	) -> BoxFuture<'a, tg::Result<Stream>>;

	fn checkin_with_options<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkin::Arg,
		options: Options,
	) -> BoxFuture<'a, tg::Result<Stream>>;

	fn checkin_request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
