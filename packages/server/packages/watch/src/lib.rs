use {
	dashmap::DashMap,
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{
		Session,
		watch::{Id, Key, Message, Watch},
	},
};

pub mod delete;
pub mod list;
pub mod touch;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub next_id: AtomicU64,
	pub watches: DashMap<Key, Watch, fnv::FnvBuildHasher>,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		// Create the watches.
		let next_id = AtomicU64::new(0);
		let watches = DashMap::default();
		let state = State { next_id, watches };

		Self(Arc::new(state))
	}

	#[must_use]
	pub fn create_watch_id(&self) -> Id {
		Id::new(self.next_id.fetch_add(1, Ordering::Relaxed))
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::GET, ["watches"]) => self.list_watches_request(session, request).await,
			(http::Method::DELETE, ["watches"]) => {
				self.try_delete_watch_request(session, request).await
			},
			(http::Method::POST, ["watches", "touch"]) => {
				self.touch_watch_request(session, request).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Remove the watches.
			self.watches.clear();
		}
		.boxed()
	}

	fn abort(&self) {
		self.watches.clear();
	}
}

impl tangram_server_core::watch::Component for Component {
	fn create_watch_id(&self) -> Id {
		self.create_watch_id()
	}

	fn watches(&self) -> &DashMap<Key, Watch, fnv::FnvBuildHasher> {
		&self.watches
	}

	fn list_watches<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::watch::list::Output>> {
		self.list_watches(session, arg).boxed()
	}

	fn try_delete_watch<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::watch::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_watch(session, arg).boxed()
	}

	fn touch_watch<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::watch::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.touch_watch(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
