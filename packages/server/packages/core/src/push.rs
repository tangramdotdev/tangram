use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	std::{collections::BTreeSet, sync::Arc, sync::Mutex},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub struct PushOrPullForProcessArg<'a> {
	pub destination: tg::Location,
	pub join: Option<tg::sync::Join>,
	pub push_arg: &'a tg::push::Arg,
	pub retry: tangram_futures::retry::Options,
	pub source: tg::Location,
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

/// The push component.
pub trait Component: crate::component::Lifecycle {
	fn push<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::push::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	>;

	fn push_for_process<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::push::Arg,
		sync: Option<tg::Referent<tg::sync::Id>>,
		join: Option<tg::sync::Join>,
		retry: tangram_futures::retry::Options,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn push_or_pull<'a>(
		&'a self,
		session: &'a Session,
		arg: &'a tg::push::Arg,
		source: tg::Location,
		destination: tg::Location,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	>;

	fn push_or_pull_for_process<'a>(
		&'a self,
		session: &'a Session,
		arg: PushOrPullForProcessArg<'a>,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::push::Header,
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
		)>,
	>;

	fn push_or_pull_with_selectors<'a>(
		&'a self,
		session: &'a Session,
		arg: &'a tg::push::Arg,
		get: Vec<tg::Referent<tg::Selector<tg::Id>>>,
		source: tg::Location,
		destination: tg::Location,
	) -> BoxFuture<
		'a,
		tg::Result<(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>,
			Arc<Mutex<BTreeSet<tg::Specifier>>>,
		)>,
	>;
}
