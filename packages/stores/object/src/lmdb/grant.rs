use {
	super::{Db, Key, KeyKind, RequestSender, Store},
	crate::{Grant, GrantArg},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[derive(Clone, Copy)]
pub(super) struct CleanRequest {
	pub now: i64,
	pub ttl: u64,
}

pub(super) struct Request {
	pub created_at: i64,
	pub id: tg::object::Id,
	pub principal: tg::Principal,
	pub subtree: bool,
}

impl Store {
	pub(super) fn spawn_grant_clean_task(
		sender: &RequestSender,
		grant_ttl: u64,
	) -> tangram_futures::task::Task<()> {
		let sender = sender.clone();
		tangram_futures::task::Task::spawn(move |stopper| async move {
			let interval = std::time::Duration::from_secs(grant_ttl);
			loop {
				tokio::select! {
					() = tokio::time::sleep(interval) => {},
					() = stopper.wait() => {
						break;
					},
				}
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_secs()
					.to_i64()
					.unwrap();
				let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
				let request = super::Request::CleanGrants(CleanRequest {
					now,
					ttl: grant_ttl,
				});
				if sender.send((request, response_sender)).await.is_err() {
					break;
				}
				let _ = response_receiver.await;
			}
		})
	}

	pub(super) fn task_clean_grants(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: &CleanRequest,
	) -> tg::Result<()> {
		let max_created_at = request.now - request.ttl.to_i64().unwrap();
		let prefix = (KeyKind::ObjectGrantCreatedAt.to_i32().unwrap(),).pack_to_vec();
		let iter = db
			.prefix_iter(&*transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate object grant indexes"))?;
		let mut expired = Vec::new();
		for entry in iter {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the object grant index"))?;
			let key = Key::unpack_object_grant_created_at(key)?;
			let (created_at, id, principal) = key;
			if created_at > max_created_at {
				break;
			}
			expired.push((created_at, id, principal));
		}
		for (created_at, id, principal) in expired {
			let grant_key = Key::ObjectGrant(&id, &principal);
			db.delete(transaction, &grant_key.pack_to_vec())
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object grant"))?;
			let index_key = Key::ObjectGrantCreatedAt(created_at, &id, &principal);
			db.delete(transaction, &index_key.pack_to_vec()).map_err(
				|error| tg::error!(!error, %id, "failed to delete the object grant index"),
			)?;
		}
		Ok(())
	}

	pub(super) async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::Grant(Request {
			created_at: arg.created_at,
			id: arg.id,
			principal: arg.principal,
			subtree: arg.subtree,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	pub(super) async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::GrantBatch(
			args.into_iter()
				.map(|arg| Request {
					created_at: arg.created_at,
					id: arg.id,
					principal: arg.principal,
					subtree: arg.subtree,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn grant_sync(&self, arg: GrantArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::task_put_object_grant(
			&self.db,
			&mut transaction,
			&arg.id,
			&arg.principal,
			arg.subtree,
			arg.created_at,
		)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn grant_batch_sync(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			Self::task_put_object_grant(
				&self.db,
				&mut transaction,
				&arg.id,
				&arg.principal,
				arg.subtree,
				arg.created_at,
			)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(super) fn task_put_object_grant(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		subtree: bool,
		created_at: i64,
	) -> tg::Result<()> {
		let principal = principal.to_string();
		let key = Key::ObjectGrant(id, &principal);
		let key_bytes = key.pack_to_vec();
		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object grant"))?
			.map(Grant::deserialize)
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object grant"))?;

		if let Some(existing) = &existing {
			let index_key = Key::ObjectGrantCreatedAt(existing.created_at, id, &principal);
			db.delete(transaction, &index_key.pack_to_vec()).map_err(
				|error| tg::error!(!error, %id, "failed to delete the object grant index"),
			)?;
		}

		let grant = Grant {
			created_at,
			subtree: subtree || existing.is_some_and(|grant| grant.subtree),
		};
		let value_bytes = grant.serialize()?;
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object grant"))?;
		let index_key = Key::ObjectGrantCreatedAt(created_at, id, &principal);
		db.put(transaction, &index_key.pack_to_vec(), &[])
			.map_err(|error| tg::error!(!error, %id, "failed to put the object grant index"))?;

		Ok(())
	}
}
