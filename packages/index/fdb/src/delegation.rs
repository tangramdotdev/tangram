use {
	crate::{Index, Key as IndexKey, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, pin_mut},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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
	pub(crate) async fn delete_delegations_for_subject_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		subject: &tg::authorization::Subject,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::DelegationSubject.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let entries = crate::propagate!(
			Self::get_delegations_with_prefix(txn, subspace, &prefix, usize::MAX).await
		);
		for arg in entries {
			for key in keys(&arg) {
				let key = Self::pack(subspace, &IndexKey::Delegation(key));
				txn.clear(&key);
			}
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&node(&arg.resource)?,
				&crate::update::Kind::Permission(arg.subject),
				crate::update::Source::Put,
				partition_total,
			);
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn get_delegations_with_prefix(
		txn: &crate::Transaction,
		_subspace: &fdbt::Subspace,
		prefix: &[u8],
		limit: usize,
	) -> tg::Result<ControlFlow<Vec<tangram_index::delegation::put::Arg>, fdb::FdbError>> {
		let range = fdb::RangeOption {
			limit: (limit != usize::MAX).then_some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix.to_vec()))
		};
		let entries = txn.get_ranges_keyvalues(range, false);
		pin_mut!(entries);
		let mut output = Vec::new();
		while let Some(result) = entries.next().await {
			let entry = crate::retry!(result);
			output.push(
				tangram_serialize::from_slice(entry.value())
					.map_err(|error| tg::error!(!error, "failed to deserialize a delegation"))?,
			);
		}
		Ok(ControlFlow::Break(output))
	}
	pub(crate) async fn put_delegation_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::delegation::put::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		if let Some(version) = &arg.version {
			let tg::authorization::Subject::Tag(id) = &arg.subject else {
				unreachable!()
			};
			let tag =
				crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, id).await);
			if tag.is_none_or(|tag| tag.version != *version) {
				return Ok(ControlFlow::Break(()));
			}
		}

		let key = IndexKey::Delegation(keys(arg)[0].clone());
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let old = crate::retry!(result);
		if let Some(value) = old {
			let old: tangram_index::delegation::put::Arg = tangram_serialize::from_slice(&value)
				.map_err(|error| tg::error!(!error, "failed to deserialize a delegation"))?;
			if old.expires_at >= arg.expires_at {
				return Ok(ControlFlow::Break(()));
			}
			let key = IndexKey::Delegation(keys(&old)[2].clone());
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		let value = tangram_serialize::to_vec(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize a delegation"))?;
		for key in keys(arg) {
			let key = Self::pack(subspace, &IndexKey::Delegation(key));
			txn.set(&key, &value);
		}
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			&node(&arg.resource)?,
			&crate::update::Kind::Permission(arg.subject.clone()),
			crate::update::Source::Put,
			partition_total,
		);
		Ok(ControlFlow::Break(()))
	}
	pub(crate) async fn delete_expired_delegations(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		now: i64,
		limit: usize,
		partition_total: u64,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		if limit == 0 {
			return Ok(ControlFlow::Break(0));
		}
		let prefix = Self::pack(subspace, &(Kind::DelegationExpiresAt.to_i32().unwrap(),));
		let entries = crate::propagate!(
			Self::get_delegations_with_prefix(txn, subspace, &prefix, limit).await
		);
		let mut count = 0;
		for arg in entries {
			if arg.expires_at > now {
				break;
			}
			for key in keys(&arg) {
				let key = Self::pack(subspace, &IndexKey::Delegation(key));
				txn.clear(&key);
			}
			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&node(&arg.resource)?,
				&crate::update::Kind::Permission(arg.subject.clone()),
				crate::update::Source::Put,
				partition_total,
			);
			count += 1;
		}
		Ok(ControlFlow::Break(count))
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
