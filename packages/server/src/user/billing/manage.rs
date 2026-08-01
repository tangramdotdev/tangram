use {
	crate::{Session, billing::CreateCustomerArg},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> tg::Result<tg::user::billing::manage::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.manage_user_billing_local().await,
			tg::Location::Remote(remote) => self.manage_user_billing_remote(arg, remote).await,
		}
	}

	async fn manage_user_billing_local(&self) -> tg::Result<tg::user::billing::manage::Output> {
		// Get the current user and the billing provider.
		let tg::Principal::User(user) = &self.context.principal else {
			return Err(tg::error!("not logged in"));
		};
		let stripe = self
			.server
			.billing
			.clone()
			.ok_or_else(|| tg::error!("billing is not configured"))?;

		// Get or create the Stripe customer.
		let user = user.clone();
		let (batch, stripe_customer_id) = self
			.server
			.database
			.run(|transaction| {
				let server = self.server.clone();
				let stripe = stripe.clone();
				let user = user.clone();
				async move {
					#[derive(db::row::Deserialize)]
					struct Row {
						name: String,
						#[tangram_database(as = "db::value::FromStr")]
						specifier: tg::Specifier,
						stripe_customer_id: Option<String>,
						stripe_default_payment_method_id: Option<String>,
					}

					let p = transaction.p();
					let statement = formatdoc!(
						"
							select users.name, specifiers.specifier, users.stripe_customer_id,
								users.stripe_default_payment_method_id
							from users
							join specifiers on specifiers.id = users.id
							where users.id = {p}1;
						"
					);
					let row = transaction
						.query_one_into::<Row>(statement.into(), db::params![user.to_string()])
						.await
						.map_err(|error| tg::error!(!error, "failed to get the user"))?;
					let configured = row.stripe_customer_id.is_some();
					let stripe_customer_id = if let Some(stripe_customer_id) =
						row.stripe_customer_id
					{
						stripe_customer_id
					} else {
						#[derive(db::row::Deserialize)]
						struct EmailRow {
							email: String,
						}

						let statement = formatdoc!(
							"
									select email
									from user_emails
									where \"user\" = {p}1
									order by email
									limit 1;
								"
						);
						let email = transaction
							.query_optional_into::<EmailRow>(
								statement.into(),
								db::params![user.to_string()],
							)
							.await
							.map_err(|error| tg::error!(!error, "failed to get the user email"))?
							.map(|row| row.email);
						let metadata =
							BTreeMap::from([("tangram_user_id".to_owned(), user.to_string())]);
						let stripe_customer_id = stripe
							.create_customer(CreateCustomerArg {
								email,
								idempotency_key: format!("tangram-user-{user}"),
								metadata,
								name: row.name,
							})
							.await?;

						let statement =
							format!("update users set stripe_customer_id = {p}1 where id = {p}2;");
						transaction
							.execute(
								statement.into(),
								db::params![stripe_customer_id.clone(), user.to_string()],
							)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to manage the Stripe customer ID")
							})?;

						stripe_customer_id
					};

					let billing = tangram_index::billing::Status::from_parts(
						true,
						configured && row.stripe_default_payment_method_id.is_some(),
					);
					let batch = tangram_index::batch::Arg {
						items: vec![tangram_index::batch::Item::PutUser(
							tangram_index::user::put::Arg {
								billing,
								id: user,
								specifier: row.specifier,
							},
						)],
					};
					server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;

					Ok::<_, crate::database::Error>(ControlFlow::Break((batch, stripe_customer_id)))
				}
				.boxed()
			})
			.await?;
		self.server.index.batch(batch).await?;

		// Create the Stripe portal session.
		let url = stripe
			.create_payment_method_update(&stripe_customer_id)
			.await?;
		let output = tg::user::billing::manage::Output { url };

		Ok(output)
	}

	async fn manage_user_billing_remote(
		&self,
		mut arg: tg::user::billing::manage::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::user::billing::manage::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.manage_user_billing(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to manage the user billing"),
		)?;

		Ok(output)
	}

	pub(crate) async fn manage_user_billing_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.manage_user_billing(arg).await?;
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.bytes(body)
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
