use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub use tangram_server_checkpoint_core::State;

/// The checkpoint component.
pub trait Component: crate::component::Lifecycle {
	fn try_abort_checkpoint<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::abort::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_continue_checkpoint_hit<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_panic_checkpoint<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::panic::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_unwatch_checkpoint<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		watch: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_wait_checkpoint_hit<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::wait::Output>>>;

	fn try_watch_checkpoint<'a>(
		&'a self,
		session: &'a Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::watch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::watch::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}

impl Server {
	pub async fn checkpoint<F>(&self, checkpoint: &str, params: F)
	where
		F: FnOnce() -> tg::checkpoint::Params,
	{
		let Some(checkpoints) = self.checkpoints.as_ref() else {
			return;
		};
		checkpoints.hit(checkpoint, params()).await;
	}
}

#[macro_export]
macro_rules! checkpoint {
	({ $params:ident }) => {};
	({ $params:ident },) => {};
	({ $params:ident }, %$name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $name.to_string().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, %$name:ident) => {
		$params.insert(stringify!($name).to_owned(), $name.to_string().into());
	};
	({ $params:ident }, ?$name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $name).into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, ?$name:ident) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $name).into());
	};
	({ $params:ident }, $name:ident = %$value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $value.to_string().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = %$value:expr) => {
		$params.insert(stringify!($name).to_owned(), $value.to_string().into());
	};
	({ $params:ident }, $name:ident = ?$value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $value).into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = ?$value:expr) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $value).into());
	};
	({ $params:ident }, $name:ident = $value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $value.into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = $value:expr) => {
		$params.insert(stringify!($name).to_owned(), $value.into());
	};
	({ $params:ident }, $name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $name.clone().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident) => {
		$params.insert(stringify!($name).to_owned(), $name.clone().into());
	};
	($server:expr, $checkpoint:expr $(,)?) => {
		$server.checkpoint($checkpoint, tg::checkpoint::Params::new)
	};
	($server:expr, $checkpoint:expr, $($arg:tt)*) => {
		$server.checkpoint($checkpoint, || {
			let mut params = tg::checkpoint::Params::new();
			$crate::checkpoint!({ params }, $($arg)*);
			params
		})
	};
}
