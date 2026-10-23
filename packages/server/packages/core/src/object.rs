use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	std::path::PathBuf,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub struct CheckoutFile {
	pub artifact: tg::artifact::Id,
	pub file: std::fs::File,
	pub path: Option<PathBuf>,
}

/// The object component.
pub trait Component: crate::component::Lifecycle {
	fn compute_object_availability<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		storage: tg::object::storage::Set,
		tokens: &'a [tg::authorization::Token],
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Availability>>>;

	fn post_object_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::object::batch::Arg,
	) -> BoxFuture<'a, tg::Result<tg::object::batch::Output>>;

	fn put_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<tg::object::put::Output>>;

	fn try_get_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn try_get_object_availability<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::availability::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Availability>>>;

	fn try_get_object_batch_local<'a>(
		&'a self,
		session: &'a Session,
		objects: &'a [tg::Referent<tg::object::Id>],
		permissions: &'a [tg::authorization::permission::Set],
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::object::get::Output>>>>;

	fn try_get_object_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		metadata: bool,
		availability: bool,
		tokens: &'a [tg::authorization::Token],
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn try_get_object_metadata<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>>;

	fn try_get_object_regions<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		regions: &'a [String],
		metadata: bool,
		availability: bool,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn try_touch_object<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn put_object_with_server<'a>(
		&'a self,
		server: &'a Server,
		arg: crate::cache::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn put_object_and_index<'a>(
		&'a self,
		server: &'a Server,
		object: crate::cache::object::put::Arg,
		index: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn put_object_batch<'a>(
		&'a self,
		server: &'a Server,
		args: Vec<crate::cache::object::put::Arg>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn put_object_batch_and_index<'a>(
		&'a self,
		server: &'a Server,
		objects: Vec<crate::cache::object::put::Arg>,
		index: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_object_batch_local_with_server<'a>(
		&'a self,
		server: &'a Server,
		ids: &'a [tg::object::Id],
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::object::get::Output>>>>;

	fn try_get_object_local_with_server<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::object::Id,
		metadata: bool,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn try_get_object_sync<'a>(
		&'a self,
		server: &'a Server,
		id: &'a tg::object::Id,
		checkout_file: &'a mut Option<CheckoutFile>,
	) -> tg::Result<Option<tg::object::get::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn wait_remote_object_put_tasks(&self) -> BoxFuture<'_, ()>;
}

impl Session {
	pub fn mask_object_metadata_with_permissions(
		metadata: tg::object::Metadata,
		permissions: tg::authorization::permission::Set,
	) -> Option<tg::object::Metadata> {
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		if permissions.contains(subtree) {
			return Some(metadata);
		}
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		permissions
			.contains(node)
			.then(tg::object::Metadata::default)
	}
}
