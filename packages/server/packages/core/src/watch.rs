use {
	crate::Session, dashmap::DashMap, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub use tangram_server_watch_core::{
	Expire, Id, Key, LockWriteGuard, Message, NewArg, Snapshot, State, UpdateArg, Watch,
};

/// The watch component.
pub trait Component: crate::component::Lifecycle {
	fn create_watch_id(&self) -> Id;

	fn watches(&self) -> &DashMap<Key, Watch, fnv::FnvBuildHasher>;

	fn list_watches<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::watch::list::Output>>;

	fn try_delete_watch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::watch::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn touch_watch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::watch::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
