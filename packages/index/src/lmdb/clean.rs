mod key;
pub(super) use key::{ItemKind, Key};

use {
	super::{Db, Index, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

struct Candidate {
	touched_at: i64,
	item: Item,
}

#[derive(Clone)]
enum Item {
	AccountObject {
		account: crate::usage::Account,
		object: tg::object::Id,
	},
	AccountProcess {
		account: crate::usage::Account,
		process: tg::process::Id,
	},
	Checkout(tg::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
	Sandbox(tg::sandbox::Id),
}

pub(super) struct TransactionArg<'a, 'b> {
	pub batch_size: usize,
	pub db: &'a Db,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
	pub max_sandbox_touched_at: i64,
	pub now: i64,
	pub subspace: &'a fdbt::Subspace,
	pub usage_partition_total: u64,
	pub transaction: &'a mut lmdb::RwTxn<'b>,
}

impl Index {
	pub async fn clean(&self, arg: crate::clean::Arg) -> tg::Result<crate::clean::Output> {
		let crate::clean::Arg {
			batch_size,
			max_object_touched_at,
			max_process_touched_at,
			max_sandbox_touched_at,
			now,
			partition_end: _,
			partition_start: _,
		} = arg;
		let request = Request::Clean(crate::lmdb::Clean {
			batch_size,
			max_object_touched_at,
			max_process_touched_at,
			max_sandbox_touched_at,
			now,
		});
		let response = self.send_write_request(request).await?;
		match response {
			Response::CleanOutput(output) => Ok(output),
			_ => Err(tg::error!("unexpected write response")),
		}
	}

	pub(super) fn clean_with_transaction(
		arg: TransactionArg<'_, '_>,
	) -> tg::Result<crate::clean::Output> {
		let TransactionArg {
			batch_size,
			db,
			max_object_touched_at,
			max_process_touched_at,
			max_sandbox_touched_at,
			now,
			subspace,
			usage_partition_total,
			transaction,
		} = arg;
		let grants = Self::delete_expired_grants(db, subspace, transaction, now, batch_size)?;
		let mut output = crate::clean::Output {
			grants,
			..Default::default()
		};
		let remaining_batch_size = batch_size.saturating_sub(grants);

		let prefix = &(Kind::Clean.to_i32().unwrap(),);
		let prefix = Self::pack(subspace, prefix);
		let mut candidates: Vec<Candidate> = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate clean keys"))?;
		for result in iter {
			if candidates.len() >= remaining_batch_size {
				break;
			}
			let (key, _) =
				result.map_err(|error| tg::error!(!error, "failed to read clean key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Clean(key) = key else {
				return Err(tg::error!("expected clean key"));
			};
			let (item, touched_at, max_touched_at) = match key {
				crate::lmdb::clean::Key::AccountObject {
					account,
					object,
					touched_at,
				} => (
					Item::AccountObject { account, object },
					touched_at,
					max_object_touched_at,
				),
				crate::lmdb::clean::Key::AccountProcess {
					account,
					process,
					touched_at,
				} => (
					Item::AccountProcess { account, process },
					touched_at,
					max_process_touched_at,
				),
				crate::lmdb::clean::Key::Checkout { id, touched_at } => {
					(Item::Checkout(id), touched_at, max_object_touched_at)
				},
				crate::lmdb::clean::Key::Object { id, touched_at } => {
					(Item::Object(id), touched_at, max_object_touched_at)
				},
				crate::lmdb::clean::Key::Process { id, touched_at } => {
					(Item::Process(id), touched_at, max_process_touched_at)
				},
				crate::lmdb::clean::Key::Sandbox { id, touched_at } => {
					(Item::Sandbox(id), touched_at, max_sandbox_touched_at)
				},
			};
			if touched_at > max_touched_at {
				continue;
			}
			candidates.push(Candidate { touched_at, item });
		}

		for candidate in &candidates {
			match &candidate.item {
				Item::AccountObject { account, object } => {
					Self::clean_account_object_entry(
						db,
						subspace,
						transaction,
						account,
						object,
						now,
						candidate.touched_at,
						usage_partition_total,
					)?;
					continue;
				},
				Item::AccountProcess { account, process } => {
					Self::clean_account_process_entry(
						db,
						subspace,
						transaction,
						account,
						process,
						now,
						candidate.touched_at,
						usage_partition_total,
					)?;
					continue;
				},
				Item::Checkout(_) | Item::Object(_) | Item::Process(_) | Item::Sandbox(_) => {},
			}
			let touched_at = Self::get_touched_at(db, subspace, transaction, &candidate.item)?;
			if touched_at != Some(candidate.touched_at) {
				Self::delete_clean_key(db, subspace, transaction, candidate)?;
				continue;
			}

			let reference_count = match &candidate.item {
				Item::AccountObject { .. } | Item::AccountProcess { .. } => unreachable!(),
				Item::Checkout(id) => {
					Self::compute_checkout_reference_count(db, subspace, transaction, id)?
				},
				Item::Object(id) => {
					Self::compute_object_reference_count(db, subspace, transaction, id)?
				},
				Item::Process(id) => {
					Self::compute_process_reference_count(db, subspace, transaction, id)?
				},
				Item::Sandbox(id) => {
					Self::compute_sandbox_reference_count(db, subspace, transaction, id)?
				},
			};

			let item = if reference_count > 0 {
				Self::set_reference_count(
					db,
					subspace,
					transaction,
					&candidate.item,
					reference_count,
				)?;
				None
			} else {
				Self::delete_item(db, subspace, transaction, &candidate.item)?;
				Some(candidate.item.clone())
			};

			Self::delete_clean_key(db, subspace, transaction, candidate)?;

			if let Some(item) = item {
				match item {
					Item::AccountObject { .. } | Item::AccountProcess { .. } => unreachable!(),
					Item::Checkout(id) => output.checkouts.push(id),
					Item::Object(id) => output.objects.push(id),
					Item::Process(id) => output.processes.push(id),
					Item::Sandbox(id) => output.sandboxes.push(id),
				}
			}
		}

		output.done = grants == 0 && candidates.is_empty();

		Ok(output)
	}

	fn delete_expired_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		now: i64,
		batch_size: usize,
	) -> tg::Result<usize> {
		let prefix = Self::pack(subspace, &(Kind::GrantExpiresAt.to_i32().unwrap(),));
		let iter = db
			.prefix_iter(&*transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate grant expiration keys"))?;
		let mut args = Vec::new();
		for result in iter {
			if args.len() >= batch_size {
				break;
			}
			let (key, _) = result
				.map_err(|error| tg::error!(!error, "failed to read the grant expiration key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource,
				subject,
				creator,
				permission,
				source,
			}) = key
			else {
				return Err(tg::error!("expected a grant expiration key"));
			};
			if expires_at > now {
				break;
			}
			args.push((
				crate::grant::delete::Arg {
					creator,
					expires_at: Some(expires_at),
					permissions: permission.into(),
					subject,
					resource,
				},
				source,
			));
		}
		let count = args.len();
		for (arg, source) in args {
			for permission in arg.permissions.iter() {
				Self::delete_grant_index_entry(
					db,
					subspace,
					transaction,
					&crate::lmdb::grant::GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						subject: &arg.subject,
						resource: &arg.resource,
					},
					source,
				)?;
				Self::enqueue_grant_update(
					db,
					subspace,
					transaction,
					&arg.resource,
					&arg.subject,
					permission,
				)?;
			}
		}
		Ok(count)
	}

	fn get_touched_at(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		item: &Item,
	) -> tg::Result<Option<i64>> {
		match item {
			Item::AccountObject { .. } | Item::AccountProcess { .. } => unreachable!(),
			Item::Checkout(id) => {
				let entry = Self::try_get_checkout_with_transaction(db, subspace, transaction, id)?
					.map(|entry| entry.touched_at);
				Ok(entry)
			},
			Item::Object(id) => {
				let object = Self::try_get_object_with_transaction(db, subspace, transaction, id)?
					.ok_or_else(|| tg::error!(%id, "the clean key referenced a missing object"))?;
				Ok(Some(object.touched_at))
			},
			Item::Process(id) => {
				let process =
					Self::try_get_process_with_transaction(db, subspace, transaction, id)?
						.ok_or_else(
							|| tg::error!(%id, "the clean key referenced a missing process"),
						)?;
				Ok(Some(process.touched_at))
			},
			Item::Sandbox(id) => {
				let sandbox =
					Self::try_get_sandbox_with_transaction(db, subspace, transaction, id)?
						.ok_or_else(
							|| tg::error!(%id, "the clean key referenced a missing sandbox"),
						)?;
				Ok(Some(sandbox.touched_at))
			},
		}
	}

	fn delete_clean_key(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		candidate: &Candidate,
	) -> tg::Result<()> {
		let key = match &candidate.item {
			Item::AccountObject { account, object } => {
				crate::lmdb::Key::Clean(crate::lmdb::clean::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
					touched_at: candidate.touched_at,
				})
			},
			Item::AccountProcess { account, process } => {
				crate::lmdb::Key::Clean(crate::lmdb::clean::Key::AccountProcess {
					account: account.clone(),
					process: process.clone(),
					touched_at: candidate.touched_at,
				})
			},
			Item::Checkout(id) => crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Checkout {
				id: id.clone(),
				touched_at: candidate.touched_at,
			}),
			Item::Object(id) => crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Object {
				id: id.clone(),
				touched_at: candidate.touched_at,
			}),
			Item::Process(id) => crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Process {
				id: id.clone(),
				touched_at: candidate.touched_at,
			}),
			Item::Sandbox(id) => crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Sandbox {
				id: id.clone(),
				touched_at: candidate.touched_at,
			}),
		};
		let key = Self::pack(subspace, &key);
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete clean key"))?;
		Ok(())
	}

	fn compute_checkout_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<u64> {
		let checkout_object_prefix = Self::pack(
			subspace,
			&(
				Kind::CheckoutObject.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let checkout_object_count =
			Self::count_keys_with_prefix(db, transaction, &checkout_object_prefix)?;

		let dependency_checkout_prefix = Self::pack(
			subspace,
			&(
				Kind::DependencyCheckout.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let dependency_checkout_count =
			Self::count_keys_with_prefix(db, transaction, &dependency_checkout_prefix)?;

		Ok(checkout_object_count + dependency_checkout_count)
	}

	fn compute_object_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<u64> {
		let child_object_prefix = Self::pack(
			subspace,
			&(Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()),
		);
		let child_object_count =
			Self::count_keys_with_prefix(db, transaction, &child_object_prefix)?;

		let object_process_prefix = Self::pack(
			subspace,
			&(
				Kind::ObjectProcess.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let object_process_count =
			Self::count_keys_with_prefix(db, transaction, &object_process_prefix)?;

		// Count tags referencing this object.
		let target_tag_prefix = Self::pack(
			subspace,
			&(Kind::TargetTag.to_i32().unwrap(), id.to_bytes().as_ref()),
		);
		let target_tag_count = Self::count_keys_with_prefix(db, transaction, &target_tag_prefix)?;
		let object_account_prefix = Self::pack(
			subspace,
			&(
				Kind::ObjectAccount.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let object_account_count =
			Self::count_keys_with_prefix(db, transaction, &object_account_prefix)?;

		Ok(child_object_count + object_account_count + object_process_count + target_tag_count)
	}

	fn compute_process_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<u64> {
		let child_process_prefix = Self::pack(
			subspace,
			&(Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()),
		);
		let child_process_count =
			Self::count_keys_with_prefix(db, transaction, &child_process_prefix)?;

		// Count tags referencing this process.
		let target_tag_prefix = Self::pack(
			subspace,
			&(Kind::TargetTag.to_i32().unwrap(), id.to_bytes().as_ref()),
		);
		let target_tag_count = Self::count_keys_with_prefix(db, transaction, &target_tag_prefix)?;
		let process_account_prefix = Self::pack(
			subspace,
			&(
				Kind::ProcessAccount.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let process_account_count =
			Self::count_keys_with_prefix(db, transaction, &process_account_prefix)?;

		Ok(child_process_count + process_account_count + target_tag_count)
	}

	fn compute_sandbox_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<u64> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SandboxProcess.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			),
		);
		let count = Self::count_keys_with_prefix(db, transaction, &prefix)?;

		Ok(count)
	}

	fn count_keys_with_prefix(
		db: &Db,
		transaction: &lmdb::RwTxn<'_>,
		prefix: &[u8],
	) -> tg::Result<u64> {
		let mut count = 0u64;
		let iter = db
			.prefix_iter(transaction, prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate keys with prefix"))?;
		for result in iter {
			result.map_err(|error| tg::error!(!error, "failed to read key"))?;
			count += 1;
		}
		Ok(count)
	}

	fn set_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		item: &Item,
		reference_count: u64,
	) -> tg::Result<()> {
		match item {
			Item::AccountObject { .. } | Item::AccountProcess { .. } => unreachable!(),
			Item::Checkout(id) => {
				let key =
					crate::lmdb::Key::Checkout(crate::lmdb::checkout::Key::Checkout(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get checkout"))?
				{
					let mut entry = crate::checkout::Checkout::deserialize(bytes)?;
					entry.reference_count = reference_count;
					let bytes = entry.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|error| tg::error!(!error, "failed to put checkout"))?;
				}
			},
			Item::Object(id) => {
				let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::Object(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get object"))?
				{
					let mut object = crate::object::Object::deserialize(bytes)?;
					object.reference_count = reference_count;
					let bytes = object.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|error| tg::error!(!error, "failed to put object"))?;
				}
			},
			Item::Process(id) => {
				let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get process"))?
				{
					let mut process = crate::process::Process::deserialize(bytes)?;
					process.reference_count = reference_count;
					let bytes = process.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|error| tg::error!(!error, "failed to put process"))?;
				}
			},
			Item::Sandbox(id) => {
				let key = crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get sandbox"))?
				{
					let mut sandbox = crate::sandbox::Sandbox::deserialize(bytes)?;
					sandbox.reference_count = reference_count;
					let bytes = sandbox.serialize()?;
					db.put(transaction, &key, &bytes)
						.map_err(|error| tg::error!(!error, "failed to put sandbox"))?;
				}
			},
		}
		Ok(())
	}

	fn delete_item(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		item: &Item,
	) -> tg::Result<()> {
		match item {
			Item::AccountObject { .. } | Item::AccountProcess { .. } => unreachable!(),
			Item::Checkout(id) => Self::delete_checkout(db, subspace, transaction, id),
			Item::Object(id) => Self::delete_object(db, subspace, transaction, id),
			Item::Process(id) => Self::delete_process(db, subspace, transaction, id),
			Item::Sandbox(id) => Self::delete_sandbox(db, subspace, transaction, id),
		}
	}

	pub(crate) fn delete_checkout(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<()> {
		let key = crate::lmdb::Key::Checkout(crate::lmdb::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete checkout"))?;

		let id_bytes = id.to_bytes();
		let prefix = &(
			Kind::CheckoutDependency.to_i32().unwrap(),
			id_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate checkout dependency keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) = result
				.map_err(|error| tg::error!(!error, "failed to read checkout dependency key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Checkout(crate::lmdb::checkout::Key::CheckoutDependency {
				dependency,
				..
			}) = &key
			else {
				return Err(tg::error!("expected checkout dependency key"));
			};
			let packed = Self::pack(subspace, &key);
			entries.push((packed, dependency.clone()));
		}

		for (key, _) in &entries {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete checkout dependency key"))?;
		}

		for (_, dependency) in entries {
			let key = crate::lmdb::Key::Checkout(crate::lmdb::checkout::Key::DependencyCheckout {
				dependency: dependency.clone(),
				checkout: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete dependency checkout key"))?;

			Self::decrement_checkout_reference_count(db, subspace, transaction, &dependency)?;
		}

		Ok(())
	}

	fn delete_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let resource = id.clone().into();
		Self::delete_materialized_grants_for_resource(db, subspace, transaction, &resource)?;

		let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let checkout = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get object"))?
			.and_then(|bytes| crate::object::Object::deserialize(bytes).ok())
			.and_then(|obj| obj.checkout);

		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete object"))?;

		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ObjectChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate object child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) =
				result.map_err(|error| tg::error!(!error, "failed to read object child key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectChild { child, .. }) =
				&key
			else {
				return Err(tg::error!("expected object child key"));
			};
			let packed = Self::pack(subspace, &key);
			entries.push((packed, child.clone()));
		}
		for (key, _) in &entries {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete object child key"))?;
		}

		for (_, child) in &entries {
			let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete child object key"))?;
		}
		for (_, child) in entries {
			Self::decrement_object_reference_count(db, subspace, transaction, &child)?;
		}

		if let Some(checkout) = &checkout {
			let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectCheckout {
				object: id.clone(),
				checkout: checkout.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete object checkout"))?;

			let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::CheckoutObject {
				checkout: checkout.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete checkout object"))?;

			Self::decrement_checkout_reference_count(db, subspace, transaction, checkout)?;
		}

		Ok(())
	}

	fn delete_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let resource = id.clone().into();
		Self::delete_materialized_grants_for_resource(db, subspace, transaction, &resource)?;

		let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let sandbox = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get process"))?
			.map(crate::process::Process::deserialize)
			.transpose()?
			.and_then(|process| process.sandbox);
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete process"))?;
		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate process child keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, _) =
				result.map_err(|error| tg::error!(!error, "failed to read process child key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessChild {
				child, ..
			}) = &key
			else {
				return Err(tg::error!("expected process child key"));
			};
			let packed = Self::pack(subspace, &key);
			entries.push((packed, child.clone()));
		}
		for (key, _) in &entries {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete process child key"))?;
		}

		for (_, child) in &entries {
			let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete child process key"))?;
		}
		for (_, child) in entries {
			Self::decrement_process_reference_count(db, subspace, transaction, &child)?;
		}

		let id_bytes = id.to_bytes();
		let prefix = &(Kind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate process object keys"))?;
		let mut object_entries: Vec<(Vec<u8>, tg::object::Id, crate::process::object::Kind)> =
			Vec::new();
		for result in iter {
			let (key, _) =
				result.map_err(|error| tg::error!(!error, "failed to read process object key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessObject {
				kind,
				object,
				..
			}) = &key
			else {
				return Err(tg::error!("expected process object key"));
			};
			let packed = Self::pack(subspace, &key);
			object_entries.push((packed, object.clone(), *kind));
		}
		for (key, _, _) in &object_entries {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete process object key"))?;
		}

		for (_, object, kind) in &object_entries {
			let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete object process key"))?;
			if kind.is_command() {
				let key =
					crate::lmdb::Key::Process(crate::lmdb::process::Key::CommandCacheableProcess {
						command: object.clone(),
						process: id.clone(),
					});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key).map_err(|error| {
					tg::error!(!error, "failed to delete the command cacheable process key")
				})?;
			}
		}
		for (_, object, _) in object_entries {
			Self::decrement_object_reference_count(db, subspace, transaction, &object)?;
		}

		if let Some(sandbox) = sandbox {
			let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::ProcessSandbox {
				process: id.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete process sandbox"))?;

			let key = crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess {
				process: id.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete sandbox process"))?;

			Self::decrement_sandbox_reference_count(db, subspace, transaction, &sandbox)?;
		}

		Ok(())
	}

	fn delete_sandbox(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		Self::delete_sandboxes_with_transaction(db, subspace, transaction, std::slice::from_ref(id))
	}

	fn delete_materialized_grants_for_resource(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<()> {
		// Collect the materialized grants.
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::ResourceGrant.to_i32().unwrap(),
			resource_bytes.as_ref(),
		);
		let prefix = Self::pack(subspace, prefix);
		let iter = db
			.prefix_iter(&*transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the resource grant keys"))?;
		let mut entries = Vec::new();
		for result in iter {
			let (key, value) = result
				.map_err(|error| tg::error!(!error, "failed to read the resource grant key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				creator,
				permission,
				subject,
				..
			}) = key
			else {
				return Err(tg::error!("expected a resource grant key"));
			};
			let value = crate::lmdb::grant::GrantValue::deserialize(value)?;
			let Some(expires_at) =
				value.source_expires_at(crate::lmdb::grant::GrantSource::Materialized)
			else {
				continue;
			};
			entries.push((creator, expires_at, permission, subject));
		}

		// Delete the materialized grants.
		for (creator, expires_at, permission, subject) in entries {
			Self::delete_grant_index_entry(
				db,
				subspace,
				transaction,
				&crate::lmdb::grant::GrantIndexEntry {
					creator: creator.as_ref(),
					expires_at,
					permission,
					subject: &subject,
					resource,
				},
				crate::lmdb::grant::GrantSource::Materialized,
			)?;
		}

		Ok(())
	}

	fn decrement_checkout_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<()> {
		let key = crate::lmdb::Key::Checkout(crate::lmdb::checkout::Key::Checkout(id.clone()));
		let key = Self::pack(subspace, &key);
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get checkout"))?
		{
			let mut entry = crate::checkout::Checkout::deserialize(bytes)?;
			let reference_count = entry.reference_count;
			if reference_count > 1 {
				entry.reference_count = reference_count - 1;
				let bytes = entry.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put checkout"))?;
			} else {
				entry.reference_count = 0;
				let bytes = entry.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put checkout"))?;

				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Checkout {
					id: id.clone(),
					touched_at: entry.touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	pub(super) fn decrement_object_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get object"))?
		{
			let mut object = crate::object::Object::deserialize(bytes)?;
			let reference_count = object.reference_count;
			if reference_count > 1 {
				object.reference_count = reference_count - 1;
				let bytes = object.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put object"))?;
			} else {
				object.reference_count = 0;
				let bytes = object.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put object"))?;

				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Object {
					id: id.clone(),
					touched_at: object.touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	pub(super) fn decrement_process_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		if let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get process"))?
		{
			let mut process = crate::process::Process::deserialize(bytes)?;
			let reference_count = process.reference_count;
			if reference_count > 1 {
				process.reference_count = reference_count - 1;
				let bytes = process.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put process"))?;
			} else {
				process.reference_count = 0;
				let bytes = process.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put process"))?;

				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Process {
					id: id.clone(),
					touched_at: process.touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put clean key"))?;
			}
		}
		Ok(())
	}

	pub(super) fn decrement_sandbox_reference_count(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let key = crate::lmdb::Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone()));
		let key = Self::pack(subspace, &key);
		let Some(bytes) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get sandbox"))?
		else {
			return Ok(());
		};
		let mut sandbox = crate::sandbox::Sandbox::deserialize(bytes)?;
		sandbox.reference_count = sandbox.reference_count.saturating_sub(1);
		let bytes = sandbox.serialize()?;
		db.put(transaction, &key, &bytes)
			.map_err(|error| tg::error!(!error, "failed to put sandbox"))?;

		if sandbox.reference_count == 0
			&& sandbox
				.data
				.as_ref()
				.is_some_and(|data| data.status.is_destroyed())
		{
			let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Sandbox {
				id: id.clone(),
				touched_at: sandbox.touched_at,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put clean key"))?;
		}

		Ok(())
	}
}
