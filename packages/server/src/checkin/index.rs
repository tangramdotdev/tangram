use {
	crate::{
		Server,
		checkin::{Graph, IndexCacheEntryArgs, IndexObjectArgs},
	},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Server {
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
				let dependencies = Self::checkin_compute_cache_entry_dependencies(graph, *index);
				if !dependencies.is_empty() {
					return Err(tg::error!(
						"destructive checkin with cache pointers is not supported for artifacts with dependencies"
					));
				}
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
				put_index_cache_entry_args.push(tangram_index::PutCacheEntryArg {
					id,
					touched_at,
					dependencies,
				});
			} else {
				// Add cache entry args.
				for arg in index_cache_entry_args {
					put_index_cache_entry_args.push(tangram_index::PutCacheEntryArg {
						id: arg.id,
						touched_at: arg.touched_at,
						dependencies: arg.dependencies,
					});
				}
			}
		}

		// Create put object args in reverse topological order.
		let put_index_object_args: Vec<_> = index_object_args.into_values().rev().collect();

		// Index.
		self.index
			.put(tangram_index::PutArg {
				cache_entries: put_index_cache_entry_args,
				objects: put_index_object_args,
				..Default::default()
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to index"))?;

		Ok(())
	}
}
