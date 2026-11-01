use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn list_processes_for_principal_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::process::list::Arg,
		kind: Kind,
	) -> tg::Result<ControlFlow<tangram_index::process::list::Output, fdb::FdbError>> {
		// Get the page of process keys for the principal, reading one more key than the limit to determine whether there is another page.
		let principal = arg.principal.to_string();
		let prefix = if let Some(status) = arg.status {
			Self::pack(
				subspace,
				&(
					kind.to_i32().unwrap(),
					principal.as_str(),
					status.to_string(),
				),
			)
		} else {
			Self::pack(subspace, &(kind.to_i32().unwrap(), principal.as_str()))
		};
		let (begin, end) = Subspace::from_bytes(prefix).range();
		let begin = if let Some(position) = &arg.after {
			let after = Self::pack(
				subspace,
				&(
					kind.to_i32().unwrap(),
					principal.as_str(),
					position.status.to_string(),
					position.process.to_bytes().as_ref(),
				),
			);
			fdb::KeySelector::first_greater_than(after)
		} else {
			fdb::KeySelector::first_greater_or_equal(begin)
		};
		let range = fdb::RangeOption {
			begin,
			end: fdb::KeySelector::first_greater_or_equal(end),
			limit: Some(arg.limit.saturating_add(1)),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let more = entries.len() > arg.limit;
		let positions = entries
			.iter()
			.take(arg.limit)
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				match key {
					Key::Process(crate::process::Key::CreatorProcess {
						process, status, ..
					}) if kind == Kind::CreatorProcess => {
						Ok(tangram_index::process::list::Position { process, status })
					},
					Key::Process(crate::process::Key::OwnerProcess {
						process, status, ..
					}) if kind == Kind::OwnerProcess => {
						Ok(tangram_index::process::list::Position { process, status })
					},
					_ => Err(tg::error!("unexpected key type")),
				}
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(entries);
		let next = if more {
			positions.last().cloned()
		} else {
			None
		};

		// Get each process and its data, skipping the processes that are missing.
		let result =
			futures::future::try_join_all(positions.into_iter().map(|position| async move {
				let process = crate::propagate!(
					Self::try_get_process_with_transaction(txn, subspace, &position.process).await
				);
				let data = crate::propagate!(
					Self::try_get_process_data_with_transaction(txn, subspace, &position.process)
						.await
				);
				let value = process
					.zip(data)
					.map(|(process, data)| (position.process, process, data));
				Ok::<_, tg::Error>(ControlFlow::Break(value))
			}))
			.await;
		let results = result?;
		let mut data = Vec::with_capacity(results.len());
		for result in results {
			let value = match result {
				ControlFlow::Break(value) => value,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			data.extend(value);
		}
		let output = tangram_index::process::list::Output { data, next };

		Ok(ControlFlow::Break(output))
	}
}
