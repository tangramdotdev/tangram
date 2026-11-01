use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The grant component.
pub trait Component: crate::component::Lifecycle {
	fn grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::grant::Arg,
	) -> BoxFuture<'a, tg::Result<tg::grant::grant::Output>>;

	fn revoke<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::revoke::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_delete_grant<'a>(
		&'a self,
		session: &'a Session,
		grant: &'a tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_get_grant<'a>(
		&'a self,
		session: &'a Session,
		grant: &'a tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::get::Output>>>;

	fn list_grants<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::list::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::grant::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(tg::grant::Data, bool), crate::database::Error>>>;

	fn revoke_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::revoke::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<()>, crate::database::Error>>>;

	fn delete_node_grants_batch_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		ids: &'a [tg::Id],
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn delete_node_grants_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::Id,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn list_resource_grants_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		resource: &'a tg::Id,
		after: Option<&'a tg::grant::Id>,
		limit: Option<u64>,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Vec<tg::grant::Data>, crate::database::Error>>>;

	fn resolve_subject_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		subject: &'a tg::authorization::subject::Selector,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::authorization::Subject>, crate::database::Error>>,
	>;
}
