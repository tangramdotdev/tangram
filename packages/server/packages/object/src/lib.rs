use {
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{Server, Session},
};

mod availability;
mod batch;
mod get;
mod metadata;
mod put;
mod storage;
mod touch;

type Tasks = tangram_futures::task::Map<
	TaskKey,
	tg::Result<Option<tg::object::get::Output>>,
	(),
	fnv::FnvBuildHasher,
>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

#[expect(clippy::struct_field_names)]
pub struct State {
	archive_tasks: tangram_futures::task::Set<tg::Result<()>>,
	get_tasks: Tasks,
	remote_put_tasks: tangram_futures::task::Set<()>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TaskKey {
	availability: bool,
	id: tg::object::Id,
	location: tg::Location,
	metadata: bool,
	tokens: tg::authorization::Tokens,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		let archive_tasks = tangram_futures::task::Set::default();
		let get_tasks = Tasks::default();
		let remote_put_tasks = tangram_futures::task::Set::default();
		let state = State {
			archive_tasks,
			get_tasks,
			remote_put_tasks,
		};

		Self(Arc::new(state))
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
			(http::Method::GET, ["objects", object, "metadata"]) => {
				self.try_get_object_metadata_request(session, request, object)
					.await
			},
			(http::Method::GET, ["objects", object, "availability"]) => {
				self.try_get_object_availability_request(session, request, object)
					.await
			},
			(http::Method::GET, ["objects", object]) => {
				self.try_get_object_request(session, request, object).await
			},
			(http::Method::PUT, ["objects", object]) => {
				self.put_object_request(session, request, object).await
			},
			(http::Method::POST, ["objects", "batch"]) => {
				self.post_object_batch_request(session, request).await
			},
			(http::Method::POST, ["objects", object, "touch"]) => {
				self.try_touch_object_request(session, request, object)
					.await
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
	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the get tasks.
			self.get_tasks.abort_all();
			let results = self.get_tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "an object get task panicked");
				}
			}
			tracing::trace!("object get tasks");

			// Abort the remote put tasks.
			self.remote_put_tasks.abort_all();
			self.remote_put_tasks.wait().await;
			tracing::trace!("remote object put tasks");
		}
		.boxed()
	}

	fn abort(&self) {
		self.archive_tasks.abort_all();
		self.get_tasks.abort_all();
		self.remote_put_tasks.abort_all();
	}
}

impl tangram_server_core::object::Component for Component {
	fn archive_object_batch<'a>(
		&'a self,
		server: &'a Server,
		args: Vec<tangram_archive::object::put::Arg>,
	) -> BoxFuture<'a, tg::Result<()>> {
		Self::archive_object_batch(server, args).boxed()
	}

	fn wait_archive_tasks(&self) -> BoxFuture<'_, ()> {
		async move {
			self.archive_tasks.wait().await;
		}
		.boxed()
	}

	fn post_object_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::object::batch::Arg,
	) -> BoxFuture<'a, tg::Result<tg::object::batch::Output>> {
		self.post_object_batch(session, arg).boxed()
	}

	fn put_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<tg::object::put::Output>> {
		self.put_object(session, id, arg).boxed()
	}

	fn try_get_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(session, id, arg).boxed()
	}

	fn try_get_object_availability<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::availability::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Availability>>> {
		self.try_get_object_availability(session, id, arg).boxed()
	}

	fn try_get_object_batch_local<'a>(
		&'a self,
		session: &'a Session,
		objects: &'a [tg::Referent<tg::object::Id>],
		permissions: &'a [tg::authorization::permission::Set],
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::object::get::Output>>>> {
		self.try_get_object_batch_local(session, objects, permissions, metadata)
			.boxed()
	}

	fn try_get_object_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		metadata: bool,
		availability: bool,
		tokens: &'a [tg::authorization::Token],
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object_local(session, id, metadata, availability, tokens)
			.boxed()
	}

	fn try_get_object_metadata<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(session, id, arg).boxed()
	}

	fn try_get_object_regions<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		regions: &'a [String],
		metadata: bool,
		availability: bool,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object_regions(session, id, regions, metadata, availability, tokens)
			.boxed()
	}

	fn try_touch_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_touch_object(session, id, arg).boxed()
	}

	fn put_object_with_server<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_server_core::cache::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object_with_server(server, arg).boxed()
	}

	fn put_object_and_index<'a>(
		&'a self,
		server: &'a Server,
		object: tangram_server_core::cache::object::put::Arg,
		index: tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object_and_index(server, object, index).boxed()
	}

	fn put_object_batch<'a>(
		&'a self,
		server: &'a Server,
		args: Vec<tangram_server_core::cache::object::put::Arg>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object_batch(server, args).boxed()
	}

	fn put_object_batch_and_index<'a>(
		&'a self,
		server: &'a Server,
		objects: Vec<tangram_server_core::cache::object::put::Arg>,
		index: tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object_batch_and_index(server, objects, index)
			.boxed()
	}

	fn try_get_object_batch_local_with_server<'a>(
		&'a self,
		server: &'a Server,
		ids: &'a [tg::object::Id],
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::object::get::Output>>>> {
		self.try_get_object_batch_local_with_server(server, ids, metadata)
			.boxed()
	}

	fn try_get_object_local_with_server<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::object::Id,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object_local_with_server(server, id, metadata)
			.boxed()
	}

	fn try_get_object_sync<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::object::Id,
		checkout_file: &'a mut Option<tangram_server_core::object::CheckoutFile>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		Self::try_get_object_sync(server, id, checkout_file)
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}

	fn wait_remote_object_put_tasks(&self) -> BoxFuture<'_, ()> {
		async move {
			self.remote_put_tasks.wait().await;
		}
		.boxed()
	}
}
