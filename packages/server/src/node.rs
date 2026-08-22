use {
	crate::Session, futures::TryStreamExt as _, std::pin::pin, tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _, tangram_index::Index as _,
};

mod named;

const NODE_INDEX_BATCH_SIZE: usize = 128;

pub(crate) struct IndexOutput {
	pub ids: Vec<Option<tg::Id>>,
	pub specifiers: Vec<Option<tg::Specifier>>,
}

impl Session {
	pub(crate) async fn contains_ids_from_index(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		// Query the index.
		let mut outputs = self.server.index.contains_ids(ids).await?;
		let missing = outputs
			.iter()
			.enumerate()
			.filter_map(|(index, &output)| (!output).then_some(index))
			.collect::<Vec<_>>();
		if missing.is_empty() {
			return Ok(outputs);
		}

		// Wait for indexing to catch up.
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		// Retry the missing IDs.
		let ids = missing
			.iter()
			.map(|&index| ids[index].clone())
			.collect::<Vec<_>>();
		let retries = self.server.index.contains_ids(&ids).await?;
		for (index, output) in std::iter::zip(missing, retries) {
			outputs[index] = output;
		}

		Ok(outputs)
	}

	pub(crate) async fn try_get_ids_for_specifiers_from_index(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		let output = self.try_get_nodes_from_index(&[], specifiers).await?;

		Ok(output.ids)
	}

	pub(crate) async fn try_get_nodes_from_index(
		&self,
		ids: &[tg::Id],
		specifiers: &[tg::Specifier],
	) -> tg::Result<IndexOutput> {
		// Query the index.
		let IndexOutput {
			ids: mut ids_for_specifiers,
			specifiers: mut specifiers_for_ids,
		} = self.try_get_nodes_once_from_index(ids, specifiers).await?;
		let missing_ids = specifiers_for_ids
			.iter()
			.enumerate()
			.filter_map(|(index, output)| output.is_none().then_some(index))
			.collect::<Vec<_>>();
		let missing_specifiers = ids_for_specifiers
			.iter()
			.enumerate()
			.filter_map(|(index, output)| output.is_none().then_some(index))
			.collect::<Vec<_>>();
		if missing_ids.is_empty() && missing_specifiers.is_empty() {
			let output = IndexOutput {
				ids: ids_for_specifiers,
				specifiers: specifiers_for_ids,
			};

			return Ok(output);
		}

		// Wait for indexing to catch up.
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		// Retry the missing IDs and specifiers.
		let retry_ids = missing_ids
			.iter()
			.map(|&index| ids[index].clone())
			.collect::<Vec<_>>();
		let retry_specifiers = missing_specifiers
			.iter()
			.map(|&index| specifiers[index].clone())
			.collect::<Vec<_>>();
		let IndexOutput {
			ids: retry_ids_for_specifiers,
			specifiers: retry_specifiers_for_ids,
		} = self
			.try_get_nodes_once_from_index(&retry_ids, &retry_specifiers)
			.await?;
		for (index, output) in std::iter::zip(missing_specifiers, retry_ids_for_specifiers) {
			ids_for_specifiers[index] = output;
		}
		for (index, output) in std::iter::zip(missing_ids, retry_specifiers_for_ids) {
			specifiers_for_ids[index] = output;
		}
		let output = IndexOutput {
			ids: ids_for_specifiers,
			specifiers: specifiers_for_ids,
		};

		Ok(output)
	}

	pub(crate) async fn try_get_nodes_once_from_index(
		&self,
		ids: &[tg::Id],
		specifiers: &[tg::Specifier],
	) -> tg::Result<IndexOutput> {
		let ids_future = async {
			let mut outputs = Vec::with_capacity(specifiers.len());
			for specifiers in specifiers.chunks(NODE_INDEX_BATCH_SIZE) {
				let batch = self
					.server
					.index
					.try_get_ids_for_specifiers(specifiers)
					.await?;
				outputs.extend(batch);
			}

			Ok::<_, tg::Error>(outputs)
		};
		let specifiers_future = async {
			let mut outputs = Vec::with_capacity(ids.len());
			for ids in ids.chunks(NODE_INDEX_BATCH_SIZE) {
				let batch = self.server.index.try_get_specifiers_for_ids(ids).await?;
				outputs.extend(batch);
			}

			Ok::<_, tg::Error>(outputs)
		};
		let (ids, specifiers) = futures::try_join!(ids_future, specifiers_future)?;
		let output = IndexOutput { ids, specifiers };

		Ok(output)
	}

	pub(crate) async fn pull_ancestors(
		&self,
		specifier: &tg::Specifier,
		pull: tg::node::AncestorsPull,
	) -> tg::Result<()> {
		if pull == tg::node::AncestorsPull::Never {
			return Ok(());
		}

		// Use the local parent when the policy only pulls missing ancestors.
		let Some(parent) = specifier.parent() else {
			return Ok(());
		};
		if pull == tg::node::AncestorsPull::Missing {
			let mut ids = self
				.try_get_ids_for_specifiers_from_index(std::slice::from_ref(&parent))
				.await?;
			if ids.pop().unwrap().is_some() {
				return Ok(());
			}
		}

		// Request all ancestors together through one sync stream per remote.
		let get = specifier
			.ancestors()
			.map(tg::Selector::Specifier)
			.map(tg::Referent::with_node)
			.collect::<Vec<_>>();
		let mut remotes = self.locations(None).await?.remotes;
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		for remote in remotes {
			let destination = tg::Location::Local(tg::location::Local::default());
			let source = tg::Location::Remote(tg::location::Remote {
				name: remote.name,
				region: None,
			});
			let arg = tg::push::Arg {
				ancestors: pull,
				destination: Some(destination.clone()),
				nodes: Vec::new(),
				source: Some(source.clone()),
				tag_targets: false,
				..Default::default()
			};
			let (stream, received_specifiers) = self
				.push_or_pull_with_selectors(&arg, get.clone(), source, destination)
				.await
				.map_err(|error| tg::error!(!error, "failed to pull the ancestors"))?;
			let mut stream = pin!(stream);
			while stream.try_next().await?.is_some() {}

			// Stop once the remote supplies the immediate parent.
			if received_specifiers.lock().unwrap().contains(&parent) {
				return Ok(());
			}
		}

		Ok(())
	}
}
