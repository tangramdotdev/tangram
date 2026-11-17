use {
	crate::{Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The checkout component.
pub trait Component: crate::component::Lifecycle {
	fn checkout<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkout::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn checkout_await_indexing<'a>(&'a self, session: &'a Session)
	-> BoxFuture<'a, tg::Result<()>>;

	fn internal_xattr_capabilities(&self, server: &Server)
	-> tg::Result<tg::file::xattrs::Options>;

	/// The lock that cleaning holds for writing from when the index deletes artifact checkouts until their paths are removed, and that a checkout holds for reading from when it uses a path in the checkouts directory until the index records the checkout.
	fn artifact_lock(&self) -> &tokio::sync::RwLock<()>;

	fn lock(&self) -> &Lock;

	fn named_checkout_maintenance_enabled(&self, server: &Server) -> bool;

	fn remove_named_checkout_entry_with_lock<'a>(
		&'a self,
		server: &'a Server,
		guard: &'a Guard<'_>,
		id: &'a tg::Id,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_named_checkout_nodes<'a>(
		&'a self,
		server: &'a Server,
		ids: &'a [tg::Id],
	) -> BoxFuture<'a, tg::Result<BTreeMap<tg::Id, NamedNode>>>;

	fn checkout_internal<'a>(
		&'a self,
		session: &'a Session,
		artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;

	/// Retain the checkouts that hold the artifacts until the process finishes, which include every item of an artifact's graph that is checked out together with it.
	fn retain_process_checkouts<'a>(
		&'a self,
		session: &'a Session,
		process: &'a tg::process::Id,
		artifacts: Vec<tg::artifact::Id>,
	) -> BoxFuture<'a, tg::Result<()>>;
}

#[derive(Default)]
pub struct Lock(tokio::sync::Mutex<()>);

pub struct Guard<'a> {
	_guard: tokio::sync::MutexGuard<'a, ()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedNode {
	pub id: tg::Id,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
	pub target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Lock {
	#[must_use]
	pub fn new() -> Self {
		Self(tokio::sync::Mutex::new(()))
	}

	pub async fn acquire(&self) -> Guard<'_> {
		let guard = self.0.lock().await;
		Guard { _guard: guard }
	}
}
