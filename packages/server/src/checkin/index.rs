use {
	crate::{
		Session,
		checkin::{Graph, IndexCacheEntryArgs, IndexObjectArgs},
	},
	num::ToPrimitive as _,
	std::path::Path,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Session {
	pub(super) async fn checkin_index(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		index_object_args: IndexObjectArgs,
		index_cache_entry_args: IndexCacheEntryArgs,
		root: &Path,
		touched_at: i64,
	) -> tg::Result<()> {
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

		// Index.
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				put_cache_entries: put_index_cache_entry_args,
				put_grants: put_grant.map(|arg| vec![arg]).unwrap_or_default(),
				put_objects: put_index_object_args,
				..Default::default()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		Ok(())
	}
}
