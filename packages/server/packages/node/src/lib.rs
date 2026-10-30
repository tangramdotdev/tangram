use {
	futures::{FutureExt as _, future::BoxFuture},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

mod index;
mod named;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::node::Component for Component {
	fn collect_named_subtrees_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		roots: &'a [tg::Id],
		batch_size: usize,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Vec<(tg::Id, tg::Specifier)>, tangram_server_core::database::Error>>,
	> {
		Self::collect_named_subtrees_with_transaction(transaction, roots, batch_size).boxed()
	}

	fn contains_ids_from_index<'a>(
		&'a self,
		session: &'a Session,
		ids: &'a [tg::Id],
	) -> BoxFuture<'a, tg::Result<Vec<bool>>> {
		self.contains_ids_from_index(session, ids).boxed()
	}

	fn delete_named_nodes_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		ids_and_specifiers: &'a [(tg::Id, tg::Specifier)],
		batch: &'a mut tangram_index::write::Arg,
		batch_size: usize,
	) -> BoxFuture<'a, tg::Result<std::ops::ControlFlow<(), tangram_server_core::database::Error>>>
	{
		self.delete_named_nodes_with_transaction(
			session,
			transaction,
			ids_and_specifiers,
			batch,
			batch_size,
		)
		.boxed()
	}

	fn pull_ancestors<'a>(
		&'a self,
		session: &'a Session,
		specifier: &'a tg::Specifier,
		pull: tg::node::AncestorsPull,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.pull_ancestors(session, specifier, pull).boxed()
	}

	fn try_get_ids_and_ancestors_for_specifiers<'a>(
		&'a self,
		session: &'a Session,
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<std::collections::BTreeMap<tg::Specifier, Option<tg::Id>>>> {
		self.try_get_ids_and_ancestors_for_specifiers(session, specifiers)
			.boxed()
	}

	fn try_get_ids_for_specifiers_from_index<'a>(
		&'a self,
		session: &'a Session,
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::Id>>>> {
		self.try_get_ids_for_specifiers_from_index(session, specifiers)
			.boxed()
	}

	fn try_get_nodes_from_index<'a>(
		&'a self,
		session: &'a Session,
		ids: &'a [tg::Id],
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<tangram_server_core::node::IndexOutput>> {
		self.try_get_nodes_from_index(session, ids, specifiers)
			.boxed()
	}

	fn try_resolve_named_node<'a>(
		&'a self,
		session: &'a Session,
		selector: &'a tg::Selector<tg::Id>,
	) -> BoxFuture<'a, tg::Result<Option<(tg::Id, tg::Specifier)>>> {
		self.try_resolve_named_node(session, selector).boxed()
	}
}
