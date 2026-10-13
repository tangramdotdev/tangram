use {
	crate::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_compute_usage(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: tangram_index::usage::compute::put::Arg<'_>,
		partition_total: u64,
	) -> tg::Result<()> {
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
				(
					tangram_index::usage::DeltaKind::SandboxSwapConsumption,
					usage.swap.consumption,
				),
				(
					tangram_index::usage::DeltaKind::SandboxSwapReservation,
					usage.swap.reservation,
				),
			] {
				let delta = i64::try_from(value)
					.map_err(|_| tg::error!("the resource usage is too large"))?;
				let entry = tangram_index::usage::DeltaArg {
					account: arg.account,
					at: arg.at,
					delta,
					kind,
					partition,
				};
				Self::add_usage_delta(db, subspace, transaction, entry)?;
			}
		}
		let delta = i64::try_from(arg.sandbox_count)
			.map_err(|_| tg::error!("the sandbox count usage is too large"))?;
		let entry = tangram_index::usage::DeltaArg {
			account: arg.account,
			at: arg.at,
			delta,
			kind: tangram_index::usage::DeltaKind::SandboxCount,
			partition,
		};
		Self::add_usage_delta(db, subspace, transaction, entry)?;

		Ok(())
	}
}
