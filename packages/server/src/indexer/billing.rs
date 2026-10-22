use {
	super::Indexer,
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Stopper,
};

impl Indexer {
	pub(super) async fn billing_cleanup_task(&self, stopper: &Stopper) -> tg::Result<()> {
		let poll_interval = self.server.config.indexer.billing_cleanup.poll_interval;
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			let now = self.server.clock.unix_timestamp();
			if let Err(error) = self.clean_billing_webhooks(now).await {
				tracing::error!(error = %error.trace(), "failed to clean the billing webhook events");
			}
			tokio::select! {
				() = stopper.wait() => return Ok(()),
				() = tokio::time::sleep(poll_interval) => {},
			}
		}
	}

	async fn clean_billing_webhooks(&self, now: i64) -> tg::Result<()> {
		let ttl = self
			.server
			.config
			.indexer
			.billing_cleanup
			.webhook_ttl
			.as_secs()
			.to_i64()
			.unwrap_or(i64::MAX);
		let max_created_at = now.saturating_sub(ttl);
		self.server
			.database
			.run(|transaction| {
				async move {
					Self::clean_billing_webhooks_with_transaction(transaction, max_created_at).await
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn clean_billing_webhooks_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		max_created_at: i64,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from billing_webhooks where created_at < {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![max_created_at])
			.await;
		crate::database::retry!(result, "failed to clean the billing webhook events");

		Ok(ControlFlow::Break(()))
	}
}
