use {
	crate::{
		Session,
		checkin::{Graph, IndexCacheEntryArgs, IndexObjectArgs},
	},
	num::ToPrimitive as _,
	std::path::Path,
	tangram_client::prelude::*,
	tangram_index::Index as _,
};

impl Session {
	pub(super) fn checkin_index(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		index_object_args: IndexObjectArgs,
		index_cache_entry_args: IndexCacheEntryArgs,
		root: &Path,
		touched_at: i64,
	) -> tg::Result<tangram_index::batch::Arg> {
		// Create put cache entry args.
		let mut put_index_cache_entry_args = Vec::new();
		if arg.options.cache_pointers {
			if arg.options.destructive {
				let index = graph.paths.get(root).unwrap();
				let dependencies = Self::checkin_get_cache_entry_dependencies(graph, *index);
				let id = graph
					.nodes
					.get(index)
					.unwrap()
					.id
					.as_ref()
					.unwrap()
					.clone()
					.try_into()
					.unwrap();
				put_index_cache_entry_args.push(tangram_index::cache::put::Arg {
					id,
					touched_at,
					dependencies,
				});
			} else {
				// Add cache entry args.
				for arg in index_cache_entry_args {
					put_index_cache_entry_args.push(tangram_index::cache::put::Arg {
						id: arg.id,
						touched_at: arg.touched_at,
						dependencies: arg.dependencies,
					});
				}
			}
		}

		// Create put object args in reverse topological order.
		let put_index_object_args: Vec<_> = index_object_args.into_values().rev().collect();

		// Create a subtree grant for the root object.
		let grant_expires_at = touched_at
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let grant_principal = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::grant::Principal::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_grant_principal()?),
		};
		let put_grant = grant_principal.map(|grant_principal| {
			let index = graph.paths.get(root).unwrap();
			let resource = graph.nodes.get(index).unwrap().id.as_ref().unwrap().clone();
			tangram_index::grant::put::Arg {
				created_at: touched_at,
				creator: Some(self.context.principal.clone()),
				expires_at: Some(grant_expires_at),
				permissions: tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Subtree,
				)
				.into(),
				principal: grant_principal,
				resource: resource.into(),
				time_to_touch: Some(self.server.config.object.grant_time_to_touch),
			}
		});
		let owner = self.storage_owner(&self.context.principal).await?;
		let index = graph.paths.get(root).unwrap();
		let root_object = graph.nodes.get(index).unwrap().id.as_ref().unwrap().clone();

		// Create the index batch.
		let arg = tangram_index::batch::Arg {
			items: put_index_cache_entry_args
				.into_iter()
				.map(tangram_index::batch::Item::PutCacheEntry)
				.chain(
					put_index_object_args
						.into_iter()
						.map(tangram_index::batch::Item::PutObject),
				)
				.chain(put_grant.map(tangram_index::batch::Item::PutGrant))
				.chain(owner.map(|owner| {
					tangram_index::batch::Item::PutOwnerObject(
						tangram_index::storage::put::ObjectArg {
							object: root_object,
							owner,
							touched_at,
						},
					)
				}))
				.collect(),
		};

		Ok(arg)
	}

	pub(super) fn checkin_index_task(
		&self,
		arg: tangram_index::batch::Arg,
		checkin_arg: &tg::checkin::Arg,
		root: &Path,
	) -> tangram_futures::task::Shared<tg::Result<()>> {
		let updates = checkin_arg
			.updates
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>()
			.join(",");
		self.server.index_tasks.spawn({
			let root = root.to_owned();
			let server = self.server.clone();
			|_| async move {
				crate::checkpoint!(
					server,
					"checkin.index",
					path = %root.display(),
					updates,
				)
				.await;
				let result = server
					.index
					.batch(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to index the checkin"));
				if let Err(error) = &result {
					tracing::error!(error = %error.trace(), "failed to index the checkin");
				}

				result
			}
		})
	}
}
