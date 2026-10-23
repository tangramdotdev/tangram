use {
	crate::Component,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_billing::{Billing as _, webhook::Event},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

#[derive(Clone)]
struct CustomerUpdate {
	customer: String,
	ready: bool,
}

impl Component {
	async fn process_billing_webhook(
		&self,
		session: &Session,
		billing: &tangram_server_core::billing::Billing,
		event: Event,
	) -> tg::Result<()> {
		// Skip a processed event.
		if self
			.is_billing_webhook_event_processed(session, &event.id)
			.await?
		{
			return Ok(());
		}

		// Reconcile the customer.
		let update = if let Some(customer) = event.customer {
			let ready = billing.customer_ready(&customer).await?;
			Some(CustomerUpdate { customer, ready })
		} else {
			None
		};

		// Store the projection and event.
		let created_at = session.server.clock.unix_timestamp();
		let event = event.id;
		tangram_server_core::checkpoint!(session.server, "billing.webhook.store", %event).await;
		let server = session.server.clone();
		let batch = session
			.server
			.database
			.run(|transaction| {
				let event = event.clone();
				let server = server.clone();
				let update = update.clone();
				async move {
					Self::store_billing_webhook_event_with_transaction(
						transaction,
						&server,
						&event,
						created_at,
						update.as_ref(),
					)
					.await
				}
				.boxed()
			})
			.await?;
		let Some((batch, users)) = batch else {
			return Ok(());
		};
		for user in users {
			session
				.server
				.components
				.authentication
				.invalidate_authentication(
					&session.server,
					tangram_server_core::authentication::cache::Invalidation::Principal(
						tg::Principal::User(user),
					),
				)
				.await;
		}
		session
			.server
			.spawn_publish_database_index_queue_notification_task();
		session.server.index.batch(batch).await??;

		Ok(())
	}

	async fn store_billing_webhook_event_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		server: &tangram_server_core::Server,
		event: &str,
		created_at: i64,
		update: Option<&CustomerUpdate>,
	) -> tg::Result<
		ControlFlow<
			Option<(tangram_index::batch::Arg, Vec<tg::user::Id>)>,
			tangram_server_core::database::Error,
		>,
	> {
		let p = transaction.p();
		let statement = format!(
			"insert into billing_webhooks (id, created_at) values ({p}1, {p}2) on conflict (id) do nothing;"
		);
		let result = transaction
			.execute(statement.into(), db::params![event, created_at])
			.await;
		let inserted = tangram_server_core::database::retry!(
			result,
			"failed to record the billing webhook event"
		);
		if inserted == 0 {
			return Ok(ControlFlow::Break(None));
		}

		let mut batch = tangram_index::batch::Arg::default();
		let mut affected_users = Vec::new();
		if let Some(update) = update {
			#[derive(db::row::Deserialize)]
			struct OrganizationRow {
				#[tangram_database(as = "db::value::FromStr")]
				id: tg::organization::Id,
				#[tangram_database(as = "db::value::FromStr")]
				specifier: tg::Specifier,
			}

			#[derive(db::row::Deserialize)]
			struct UserRow {
				#[tangram_database(as = "db::value::FromStr")]
				id: tg::user::Id,
				#[tangram_database(as = "db::value::FromStr")]
				specifier: tg::Specifier,
			}

			let p = transaction.p();
			let statement = format!(
				"select organizations.id, specifiers.specifier from organizations join specifiers on specifiers.id = organizations.id where organizations.billing_customer_id = {p}1;"
			);
			let result = transaction
				.query_all_into::<OrganizationRow>(
					statement.into(),
					db::params![update.customer.clone()],
				)
				.await;
			let organizations = tangram_server_core::database::retry!(
				result,
				"failed to get the billing organizations"
			);
			let statement = format!(
				"select users.id, specifiers.specifier from users join specifiers on specifiers.id = users.id where users.billing_customer_id = {p}1;"
			);
			let result = transaction
				.query_all_into::<UserRow>(statement.into(), db::params![update.customer.clone()])
				.await;
			let users =
				tangram_server_core::database::retry!(result, "failed to get the billing users");

			affected_users.extend(users.iter().map(|row| row.id.clone()));
			let billing_ready = update.ready;
			batch.items.extend(organizations.into_iter().map(|row| {
				tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
					billing_ready: Some(billing_ready),
					id: row.id,
					specifier: row.specifier,
				})
			}));
			batch.items.extend(users.into_iter().map(|row| {
				tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
					billing_ready: Some(billing_ready),
					id: row.id,
					specifier: row.specifier,
				})
			}));

			for table in ["organizations", "users"] {
				let statement = format!(
					"update {table} set billing_ready = {p}1 where billing_customer_id = {p}2;"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![update.ready, update.customer.clone()],
					)
					.await;
				tangram_server_core::database::retry!(
					result,
					"failed to update the billing customer"
				);
			}
		}

		match server
			.enqueue_database_index_queue_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(Some((batch, affected_users))))
	}

	async fn is_billing_webhook_event_processed(
		&self,
		session: &Session,
		event: &str,
	) -> tg::Result<bool> {
		let event = event.to_owned();
		let processed = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let event = event.clone();
				async move {
					Self::is_billing_webhook_event_processed_with_transaction(transaction, &event)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(processed)
	}

	async fn is_billing_webhook_event_processed_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		event: &str,
	) -> tg::Result<ControlFlow<bool, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = format!("select id from billing_webhooks where id = {p}1;");
		let result = transaction
			.query_optional_value_into::<String>(statement.into(), db::params![event])
			.await;
		let processed = tangram_server_core::database::retry!(
			result,
			"failed to get the billing webhook event"
		)
		.is_some();

		Ok(ControlFlow::Break(processed))
	}

	pub(crate) async fn handle_billing_webhook_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		if !session.server.is_primary_region() {
			return session.forward_request_to_primary_region(request).await;
		}

		// Read and verify the webhook.
		let headers = request.headers().clone();
		let body = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the billing webhook body"))?;
		let billing = self
			.provider
			.as_ref()
			.ok_or_else(|| tg::error!("billing is not configured"))?;
		let now = session.server.clock.unix_timestamp();
		let event = match billing.try_parse_webhook(&headers, &body, now) {
			Ok(Some(event)) => event,
			Ok(None) => return Ok(http::Response::builder().ok().empty().unwrap().boxed_body()),
			Err(error) => {
				tracing::warn!(%error, "failed to verify or parse the billing webhook");
				return Ok(http::Response::builder()
					.bad_request()
					.empty()
					.unwrap()
					.boxed_body());
			},
		};

		// Process the event.
		self.process_billing_webhook(session, billing, event)
			.await?;
		let response = http::Response::builder().ok().empty().unwrap().boxed_body();

		Ok(response)
	}
}
