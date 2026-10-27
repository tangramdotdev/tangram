use {
	crate::Index, foundationdb as fdb, foundationdb_tuple as fdbt, std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_compute_usage(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: tangram_index::usage::compute::put::Arg<'_>,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition = rand::random_range(0..partition_total);
		if let Some(usage) = arg.usage {
			for (kind, value) in [
				(
					tangram_index::usage::DeltaKind::SandboxCpuConsumption,
					usage.cpu.consumption,
				),
				(
					tangram_index::usage::DeltaKind::SandboxCpuReservation,
					usage.cpu.reservation,
				),
				(
					tangram_index::usage::DeltaKind::SandboxMemoryConsumption,
					usage.memory.consumption,
				),
				(
					tangram_index::usage::DeltaKind::SandboxMemoryReservation,
					usage.memory.reservation,
				),
				(
					tangram_index::usage::DeltaKind::SandboxStorageConsumption,
					usage.storage.consumption,
				),
				(
					tangram_index::usage::DeltaKind::SandboxStorageReservation,
					usage.storage.reservation,
				),
			] {
				let delta = i64::try_from(value)
					.map_err(|_| tg::error!("the resource usage is too large"))?;
				Self::add_usage_delta(txn, subspace, arg.account, arg.at, kind, delta, partition);
			}
		}
		let delta = i64::try_from(arg.sandbox_count)
			.map_err(|_| tg::error!("the sandbox count usage is too large"))?;
		Self::add_usage_delta(
			txn,
			subspace,
			arg.account,
			arg.at,
			tangram_index::usage::DeltaKind::SandboxCount,
			delta,
			partition,
		);

		Ok(ControlFlow::Break(()))
	}
}
