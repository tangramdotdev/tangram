use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The sync component.
pub trait Component: crate::component::Lifecycle {
	fn sync<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sync::Header,
					BoxStream<'static, tg::Result<tg::sync::Message>>,
				),
				tg::Error,
			>,
		>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn prepare_sync<'a>(
		&'a self,
		session: &'a Session,
		sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::Referent<tg::sync::Id>>;

	fn refresh_sync<'a>(
		&'a self,
		session: &'a Session,
		id: tg::sync::Id,
	) -> tg::Result<tg::Referent<tg::sync::Id>>;

	fn request_sync_verification<'a>(
		&'a self,
		sync: &'a tg::sync::Id,
		arg: tg::sync::service::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sync::service::Output>>>;

	fn sync_inner<'a>(
		&'a self,
		session: &'a Session,
		arg: InnerArg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sync::Header,
					BoxStream<'static, tg::Result<tg::sync::Message>>,
				),
				tg::Error,
			>,
		>,
	>;
}

#[derive(Default)]
pub struct InnerArg {
	pub arg: tg::sync::Arg,
	pub get: Option<tokio::sync::mpsc::Receiver<tg::Referent<tg::Selector<tg::Id>>>>,
	pub process: bool,
	pub trust: bool,
}
