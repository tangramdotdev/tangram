use {
	super::{Db, Index, Request, Response},
	crate::CleanOutput,
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn clean(&self, max_touched_at: i64, batch_size: usize) -> tg::Result<CleanOutput> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Clean {
			max_touched_at,
			batch_size,
		};
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		match response {
			Response::CleanOutput(output) => Ok(output),
			_ => Err(tg::error!("unexpected response")),
		}
	}

	#[expect(clippy::unnecessary_wraps)]
	pub(super) fn task_clean(
		_db: &Db,
		_transaction: &mut lmdb::RwTxn<'_>,
		_max_touched_at: i64,
		_batch_size: usize,
	) -> tg::Result<CleanOutput> {
		// The clean operation is not yet implemented.
		Ok(CleanOutput::default())
	}
}
