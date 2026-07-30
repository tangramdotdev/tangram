use {
	crate::{Session, sync::get::State},
	futures::FutureExt as _,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
};

pub struct Item {
	pub message: tg::sync::PutItemSandboxMessage,
}

impl Session {
	pub(super) async fn sync_get_sandbox(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Item>,
	) -> tg::Result<()> {
		while let Some(item) = receiver.recv().await {
			self.sync_get_sandbox_item(&state, item).await?;
		}

		Ok(())
	}

	async fn sync_get_sandbox_item(&self, state: &State, item: Item) -> tg::Result<()> {
		// Validate the sandbox.
		let mut data = item.message.data;
		if data.id != item.message.id {
			return Err(tg::error!(
				expected = %item.message.id,
				actual = %data.id,
				"invalid sandbox id"
			));
		}
		if !data.status.is_destroyed() {
			return Err(tg::error!(id = %item.message.id, "cannot sync a running sandbox"));
		}
		data.location = Some(tg::Location::Local(tg::location::Local::default()));

		// Authorize the write.
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let existing = self
			.try_get_sandbox_from_index(&item.message.id)
			.await?
			.is_some();
		if existing {
			let permission =
				tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Write);
			let authorized = self.authorize(item.message.id.clone(), permission).await?;
			if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
				return Err(tg::error!("unauthorized"));
			}
		}

		// Enqueue the sandbox and temporary grant atomically.
		let created_at = item.message.created_at;
		let id = item.message.id;
		let session = self.clone();
		let id_for_transaction = id.clone();
		self.server
			.database
			.run(|transaction| {
				let data = data.clone();
				let id = id_for_transaction.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
					batch.items.push(tangram_index::batch::Item::PutSandbox(
						tangram_index::sandbox::put::Arg {
							created_at,
							data: Some(data),
							id: id.clone(),
							runner: None,
							touched_at,
						},
					));
					if !existing
						&& let Some(arg) =
							session.sync_get_create_temporary_grant(&id.clone().into())?
					{
						batch.items.push(tangram_index::batch::Item::PutGrant(arg));
					}
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		// Update the graph.
		let id = id.into();
		state.graph.lock().unwrap().update_item_local_applied(&id);
		state.progress.increment_transferred_item(&id);
		if state.graph.lock().unwrap().end_local(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}
}
