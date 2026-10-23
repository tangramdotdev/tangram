use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub use tangram_server_clock_types::{Clock, Elapsed, Instant, Interval, Real, Sleep, Virtual};

/// The clock component.
pub trait Component: crate::component::Lifecycle {
	fn try_advance_clock<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::clock::advance::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>>;

	fn try_get_clock<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>>;

	fn try_set_clock<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::clock::set::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
