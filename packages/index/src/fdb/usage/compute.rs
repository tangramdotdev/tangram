use {crate::fdb::Index, foundationdb as fdb, foundationdb_tuple as fdbt};

impl Index {
	pub(crate) fn put_compute_usage(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: crate::usage::compute::put::Arg<'_>,
		partition_total: u64,
	) -> crate::fdb::Result<()> {
		let partition = rand::random_range(0..partition_total);
		if let Some(cpu) = arg.cpu {
			let delta = i64::try_from(cpu)
				.map_err(|_| crate::fdb::error!("the compute CPU usage is too large"))?;
			Self::add_usage_delta(
				txn,
				subspace,
				arg.account,
				arg.at,
				crate::usage::DeltaKind::SandboxCpu,
				delta,
				partition,
			);
		}
		if let Some(memory) = arg.memory {
			let delta = i64::try_from(memory)
				.map_err(|_| crate::fdb::error!("the compute memory usage is too large"))?;
			Self::add_usage_delta(
				txn,
				subspace,
				arg.account,
				arg.at,
				crate::usage::DeltaKind::SandboxMemory,
				delta,
				partition,
			);
		}
		let delta = i64::try_from(arg.sandbox_count)
			.map_err(|_| crate::fdb::error!("the sandbox count usage is too large"))?;
		Self::add_usage_delta(
			txn,
			subspace,
			arg.account,
			arg.at,
			crate::usage::DeltaKind::SandboxCount,
			delta,
			partition,
		);

		Ok(())
	}
}
