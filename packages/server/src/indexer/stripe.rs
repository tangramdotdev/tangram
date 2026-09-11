use {
	super::Indexer,
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Stopper,
};

const POLL_INTERVAL: Duration = Duration::from_hours(1);
const WEBHOOK_TTL: Duration = Duration::from_hours(35 * 24);

impl Indexer {
	pub(super) async fn stripe_cleanup_task(&self, stopper: &Stopper) -> tg::Result<()> {
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			let now = self.server.clock.unix_timestamp()?;
			if let Err(error) = self.clean_stripe_webhooks(now).await {
				tracing::error!(error = %error.trace(), "failed to clean the Stripe webhook events");
			}
			tokio::select! {
				() = stopper.wait() => return Ok(()),
				() = tokio::time::sleep(POLL_INTERVAL) => {},
			}
		}
	}

	async fn clean_stripe_webhooks(&self, now: i64) -> tg::Result<()> {
		let ttl = WEBHOOK_TTL.as_secs().to_i64().unwrap();
		let max_created_at = now.saturating_sub(ttl);
		self.server
			.database
			.run(|transaction| {
				async move {
					Self::clean_stripe_webhooks_with_transaction(transaction, max_created_at).await
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn clean_stripe_webhooks_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		max_created_at: i64,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from stripe_webhooks where created_at < {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![max_created_at])
			.await;
		crate::database::retry!(result, "failed to clean the Stripe webhook events");

		Ok(ControlFlow::Break(()))
	}
}
