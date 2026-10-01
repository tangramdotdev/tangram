use {
	crate::{Db, Index, Key as IndexKey, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};
#[derive(Clone, Debug)]
pub enum Key {
	Delegation {
		resource: tg::Id,
		source: tg::authorization::Subject,
		subject: tg::authorization::Subject,
	},
	ExpiresAt {
		expires_at: i64,
		resource: tg::Id,
		source: tg::authorization::Subject,
		subject: tg::authorization::Subject,
	},
	Source {
		resource: tg::Id,
		source: tg::authorization::Subject,
		subject: tg::authorization::Subject,
	},
	Subject {
		resource: tg::Id,
		source: tg::authorization::Subject,
		subject: tg::authorization::Subject,
	},
}

impl Index {
	pub(crate) fn delete_delegations_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		subject: &tg::authorization::Subject,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::DelegationSubject.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let entries =
			Self::get_delegations_with_prefix(db, subspace, transaction, &prefix, usize::MAX)?;
		for arg in entries {
			for key in keys(&arg) {
				let key = Self::pack(subspace, &IndexKey::Delegation(key));
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete a delegation"))?;
			}
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				node(&arg.resource)?,
				crate::update::Kind::Permission(arg.subject),
				crate::update::Source::Put,
				None,
			)?;
		}
		Ok(())
	}

	pub(crate) fn get_delegations_with_prefix(
		db: &Db,
		_subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		limit: usize,
	) -> tg::Result<Vec<tangram_index::delegation::put::Arg>> {
		let entries = db
			.prefix_iter(transaction, prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the delegations"))?;
		let mut output = Vec::new();
		for entry in entries.take(limit) {
			let (_, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a delegation"))?;
			output.push(
				tangram_serialize::from_slice(value)
					.map_err(|error| tg::error!(!error, "failed to deserialize a delegation"))?,
			);
		}
		Ok(output)
	}
	pub(crate) fn put_delegation_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::delegation::put::Arg,
	) -> tg::Result<()> {
		arg.validate()?;
		let key = IndexKey::Delegation(keys(arg)[0].clone());
		let key = Self::pack(subspace, &key);
		let old = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read a delegation"))?
			.map(<[u8]>::to_vec);
		if let Some(value) = old {
			let old: tangram_index::delegation::put::Arg = tangram_serialize::from_slice(&value)
				.map_err(|error| tg::error!(!error, "failed to deserialize a delegation"))?;
			if old.expires_at >= arg.expires_at {
				return Ok(());
			}
			let key = IndexKey::Delegation(keys(&old)[2].clone());
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key).map_err(|error| {
				tg::error!(!error, "failed to delete the delegation expiration")
			})?;
		}
		let value = tangram_serialize::to_vec(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize a delegation"))?;
		for key in keys(arg) {
			let key = Self::pack(subspace, &IndexKey::Delegation(key));
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to write a delegation"))?;
		}
		Self::enqueue_update_with_kind(
			db,
			subspace,
			transaction,
			node(&arg.resource)?,
			crate::update::Kind::Permission(arg.subject.clone()),
			crate::update::Source::Put,
			None,
		)?;
		Ok(())
	}
	pub(crate) fn delete_expired_delegations(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		now: i64,
		limit: usize,
	) -> tg::Result<usize> {
		if limit == 0 {
			return Ok(0);
		}
		let prefix = Self::pack(subspace, &(Kind::DelegationExpiresAt.to_i32().unwrap(),));
		let entries = Self::get_delegations_with_prefix(db, subspace, transaction, &prefix, limit)?;
		let mut count = 0;
		for arg in entries {
			if arg.expires_at > now {
				break;
			}
			for key in keys(&arg) {
				let key = Self::pack(subspace, &IndexKey::Delegation(key));
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete a delegation"))?;
			}
			Self::enqueue_update_with_kind(
				db,
				subspace,
				transaction,
				node(&arg.resource)?,
				crate::update::Kind::Permission(arg.subject.clone()),
				crate::update::Source::Put,
				None,
			)?;
			count += 1;
		}
		Ok(count)
	}
}

fn keys(arg: &tangram_index::delegation::put::Arg) -> [Key; 4] {
	let resource = &arg.resource;
	let source = &arg.source;
	let subject = &arg.subject;
	[
		Key::Delegation {
			resource: resource.clone(),
			source: source.clone(),
			subject: subject.clone(),
		},
		Key::Source {
			resource: resource.clone(),
			source: source.clone(),
			subject: subject.clone(),
		},
		Key::ExpiresAt {
			expires_at: arg.expires_at,
			resource: resource.clone(),
			source: source.clone(),
			subject: subject.clone(),
		},
		Key::Subject {
			resource: resource.clone(),
			source: source.clone(),
			subject: subject.clone(),
		},
	]
}

fn node(resource: &tg::Id) -> tg::Result<tg::Either<tg::object::Id, tg::process::Id>> {
	if resource.kind() == tg::id::Kind::Process {
		Ok(tg::Either::Right(resource.clone().try_into()?))
	} else {
		Ok(tg::Either::Left(resource.clone().try_into()?))
	}
}
