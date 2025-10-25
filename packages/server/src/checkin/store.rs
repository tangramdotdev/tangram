use {
	super::state::{State, Variant},
	crate::Server,
	tangram_client as tg,
	tangram_either::Either,
	tangram_store::prelude::*,
};

impl Server {
	pub(super) async fn checkin_store(&self, state: &State, touched_at: i64) -> tg::Result<()> {
		// Collect dirty objects.
		let mut dirty_objects = std::collections::HashSet::<_, tg::id::BuildHasher>::default();
		for node in state.graph.nodes.values() {
			if node.dirty {
				if let Some(object_id) = &node.object_id {
					dirty_objects.insert(object_id.clone());
					if let Some(object) = state.objects.as_ref().unwrap().get(object_id)
						&& let Some(data) = &object.data
						&& let Some(graph_id) = match data {
							tg::object::Data::Directory(tg::directory::Data::Reference(
								reference,
							))
							| tg::object::Data::File(tg::file::Data::Reference(reference))
							| tg::object::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
								reference.graph.as_ref()
							},
							_ => None,
						} {
						dirty_objects.insert(graph_id.clone().into());
					}
				}
				if let Variant::File(file) = &node.variant
					&& let Some(Either::Left(blob)) = &file.contents
				{
					dirty_objects.insert(blob.id.clone().into());
				}
			}
		}

		let args: Vec<_> = state
			.objects
			.as_ref()
			.unwrap()
			.iter()
			.filter(|(id, _)| dirty_objects.contains(id))
			.map(|(_, object)| crate::store::PutArg {
				bytes: object.bytes.clone(),
				cache_reference: object.cache_reference.clone(),
				id: object.id.clone(),
				touched_at,
			})
			.collect();
		self.store
			.put_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;

		Ok(())
	}
}
