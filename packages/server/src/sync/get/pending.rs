use {
	crate::{Session, sync::get::State},
	futures::{
		FutureExt as _, StreamExt as _,
		future::{AbortHandle, BoxFuture},
		stream::FuturesUnordered,
	},
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
};

#[derive(Default)]
pub(super) struct Pending {
	futures: FuturesUnordered<BoxFuture<'static, Option<(tg::Id, tg::Result<()>)>>>,
	nodes: BTreeMap<tg::Id, Node>,
}

struct Node {
	abort: AbortHandle,
	missing: bool,
	output: Option<tg::Result<()>>,
}

impl Pending {
	pub fn insert(
		&mut self,
		session: &Session,
		state: &Arc<State>,
		checkout_sender: &tokio::sync::mpsc::Sender<super::checkout::ObjectNode>,
		id: tg::Id,
	) {
		if self.nodes.contains_key(&id) {
			return;
		}
		state.pending.lock().unwrap().insert(id.clone());
		let session = session.clone();
		let state = state.clone();
		let checkout_sender = checkout_sender.clone();
		let node_id = id.clone();
		let future = async move {
			let output = session
				.sync_get_pending(&state, &checkout_sender, node_id.clone())
				.await;
			(node_id, output)
		};
		let (future, abort) = futures::future::abortable(future);
		self.futures.push(async move { future.await.ok() }.boxed());
		let node = Node {
			abort,
			missing: false,
			output: None,
		};
		self.nodes.insert(id, node);
	}

	pub fn remove(&mut self, id: &tg::Id) {
		if let Some(node) = self.nodes.remove(id) {
			node.abort.abort();
		}
	}

	pub fn missing(&mut self, id: &tg::Id) -> tg::Result<bool> {
		let Some(node) = self.nodes.get_mut(id) else {
			return Ok(false);
		};
		node.missing = true;
		if let Some(output) = node.output.take() {
			output?;
		}
		Ok(true)
	}

	pub fn is_empty(&self) -> bool {
		self.futures.is_empty()
	}

	pub async fn next(&mut self) -> tg::Result<()> {
		if let Some(Some((id, output))) = self.futures.next().await
			&& let Some(node) = self.nodes.get_mut(&id)
		{
			if node.missing {
				output?;
			} else {
				// A failed fallback cannot defeat a source that is still trying.
				node.output = Some(output);
			}
		}
		Ok(())
	}

	pub async fn finish(&mut self) -> tg::Result<()> {
		for node in self.nodes.values_mut() {
			node.missing = true;
			if let Some(output) = node.output.take() {
				output?;
			}
		}
		while !self.is_empty() {
			self.next().await?;
		}
		Ok(())
	}
}

impl Session {
	pub(super) async fn sync_get_pending_available(state: &State) -> tg::Result<()> {
		let ids = state
			.pending
			.lock()
			.unwrap()
			.iter()
			.cloned()
			.collect::<Vec<_>>();
		for id in ids {
			match id.kind() {
				tg::id::Kind::Process => {
					let id = id.try_into()?;
					let availability = state
						.graph
						.lock()
						.unwrap()
						.get_process_local_availability(&id);
					Self::sync_get_index_send_process_available(state, &id, &availability).await?;
				},
				kind if kind.is_object() => {
					let id = id.try_into()?;
					let available = state
						.graph
						.lock()
						.unwrap()
						.get_object_local_availability(&id)
						.subtree;
					if available {
						Self::sync_get_index_send_object_available(state, &id).await?;
					}
				},
				_ => return Err(tg::error!(%id, "invalid pending node kind")),
			}
		}
		Ok(())
	}

	async fn sync_get_pending(
		&self,
		state: &State,
		checkout_sender: &tokio::sync::mpsc::Sender<super::checkout::ObjectNode>,
		id: tg::Id,
	) -> tg::Result<()> {
		// Flush the queued metadata before looking for a local fallback.
		crate::checkpoint!(self.server, "sync.get.pending.index", id = %id).await;
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		match id.kind() {
			tg::id::Kind::Process => {
				let id: tg::process::Id = id.try_into()?;
				let entry = state
					.graph
					.lock()
					.unwrap()
					.get_node_local_tokens(&id.clone().into());
				let tokens = tg::authorization::Tokens::with_local_entry(entry);
				let tg::authorization::permission::Set::Process(permissions) =
					Self::sync_get_process_permissions(&state.arg)
				else {
					return Err(tg::error!("expected process permissions"));
				};
				let request = tg::sync::control::ClientRequestArg::process(
					id.clone(),
					permissions,
					Some(tg::process::Storage::default()),
				);
				self.try_get_with_sync_wait(&tokens, request, |output| {
					let id = id.clone();
					async move {
						if let Some(output) = output {
							state
								.graph
								.lock()
								.unwrap()
								.update_node_local_control_output(&id.clone().into(), &output)?;
						}
						let touched_at = self.server.clock.unix_timestamp()?;
						let (mut outputs, _) = self
							.sync_get_touch_authorized_processes(
								&state.graph,
								&[id],
								&state.arg,
								touched_at,
								self.server.config.process.time_to_touch,
							)
							.await?;
						Ok(outputs.pop().flatten())
					}
				})
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
				let node = super::index::ProcessNode { id, missing: true };
				self.sync_get_index_process_batch(state, vec![node], None)
					.await?;
			},
			kind if kind.is_object() => {
				let node = super::index::ObjectNode {
					id: id.try_into()?,
					missing: true,
				};
				self.sync_get_index_object_batch(state, checkout_sender, vec![node], None)
					.await?;
			},
			_ => return Err(tg::error!(%id, "invalid pending node kind")),
		}
		Ok(())
	}
}
