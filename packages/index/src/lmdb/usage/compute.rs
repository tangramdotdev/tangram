use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_compute_usage(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: crate::usage::compute::put::Arg<'_>,
		partition_total: u64,
	) -> tg::Result<()> {
		let partition = rand::random_range(0..partition_total);
		if let Some(cpu) = arg.cpu {
			let delta =
				i64::try_from(cpu).map_err(|_| tg::error!("the compute CPU usage is too large"))?;
			Self::add_usage_delta(
				db,
				subspace,
				transaction,
				crate::usage::DeltaArg {
					account: arg.account,
					at: arg.at,
					delta,
					kind: crate::usage::DeltaKind::SandboxCpu,
					partition,
				},
			)?;
		}
		if let Some(memory) = arg.memory {
			let delta = i64::try_from(memory)
				.map_err(|_| tg::error!("the compute memory usage is too large"))?;
			Self::add_usage_delta(
				db,
				subspace,
				transaction,
				crate::usage::DeltaArg {
					account: arg.account,
					at: arg.at,
					delta,
					kind: crate::usage::DeltaKind::SandboxMemory,
					partition,
				},
			)?;
		}
		let delta = i64::try_from(arg.sandbox_count)
			.map_err(|_| tg::error!("the sandbox count usage is too large"))?;
		Self::add_usage_delta(
			db,
			subspace,
			transaction,
			crate::usage::DeltaArg {
				account: arg.account,
				at: arg.at,
				delta,
				kind: crate::usage::DeltaKind::SandboxCount,
				partition,
			},
		)?;

		Ok(())
	}
}
