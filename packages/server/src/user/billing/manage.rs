use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_billing::Billing as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
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
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.manage_user_billing_primary_region(arg).await
			},
			tg::Location::Local(_) => self.manage_user_billing_local().await,
			tg::Location::Remote(remote) => self.manage_user_billing_remote(arg, remote).await,
		}
	}

	async fn manage_user_billing_local(&self) -> tg::Result<tg::user::billing::manage::Output> {
		let tg::Principal::User(user) = &self.context.principal else {
			return Err(tg::error!("not logged in"));
		};
		let billing = self
			.server
			.billing
			.clone()
			.ok_or_else(|| tg::error!("billing is not configured"))?;

		let user = user.clone();
		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		let billing_customer_id = tangram_futures::retry(&options, || {
			let session = session.clone();
			let billing = billing.clone();
			let user = user.clone();
			async move {
				match session
					.manage_user_billing_local_attempt(&user, &billing)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;

		// Create the billing portal session.
		let url = billing.create_management_url(&billing_customer_id).await?;
		let output = tg::user::billing::manage::Output { url };

		Ok(output)
	}

	async fn manage_user_billing_local_attempt(
		&self,
		user: &tg::user::Id,
		billing: &crate::billing::Billing,
	) -> tg::Result<ControlFlow<String>> {
		let selector = tg::Selector::Id(user.clone().into());
		let Some((id, specifier)) = self.try_resolve_named_node(&selector).await? else {
			return Err(tg::error!("failed to find the user"));
		};
		let ids_by_specifier = BTreeMap::from([(specifier, Some(id))]);
		let user = user.clone();
		let data = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let user = user.clone();
				async move { Self::get_user_billing_data_with_transaction(transaction, &user).await }
				.boxed()
			})
			.await?;
		let Some((email, name, billing_customer_id)) = data else {
			return Ok(ControlFlow::Continue(()));
		};
		let billing_customer_id = if let Some(billing_customer_id) = billing_customer_id {
			billing_customer_id
		} else {
			let arg = tangram_billing::customer::create::Arg {
				account: tg::usage::Account::User(user.clone()),
				email,
				name,
			};
			billing.create_customer(arg).await?
		};
		let batch_size = self.server.config.sync.get.database.batch_size;
		let billing_customer_id = self
			.server
			.database
			.run(|transaction| {
				let ids_by_specifier = ids_by_specifier.clone();
				let billing_customer_id = billing_customer_id.clone();
				let user = user.clone();
				async move {
					Self::store_user_billing_customer_id_with_transaction(
						transaction,
						&user,
						&ids_by_specifier,
						batch_size,
						billing_customer_id,
					)
					.await
				}
				.boxed()
			})
			.await?;

		Ok(billing_customer_id)
	}

	async fn get_user_billing_data_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &tg::user::Id,
	) -> tg::Result<
		ControlFlow<Option<(Option<String>, String, Option<String>)>, crate::database::Error>,
	> {
		#[derive(db::row::Deserialize)]
		struct Row {
			billing_customer_id: Option<String>,
			name: String,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select users.name, users.billing_customer_id
				from users
				where users.id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![user.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to get the user");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};
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
		let result = transaction
			.query_optional_into::<EmailRow>(statement.into(), db::params![user.to_string()])
			.await;
		let email =
			crate::database::retry!(result, "failed to get the user email").map(|row| row.email);
		let output = (email, row.name, row.billing_customer_id);

		Ok(ControlFlow::Break(Some(output)))
	}

	async fn store_user_billing_customer_id_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &tg::user::Id,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
		batch_size: usize,
		billing_customer_id: String,
	) -> tg::Result<ControlFlow<ControlFlow<String>, crate::database::Error>> {
		match Self::verify_ids_for_specifiers_with_transaction(
			transaction,
			ids_by_specifier,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(true) => (),
			ControlFlow::Break(false) => {
				return Ok(ControlFlow::Break(ControlFlow::Continue(())));
			},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			billing_customer_id: Option<String>,
		}
		let p = transaction.p();
		let statement = format!("select billing_customer_id from users where id = {p}1;");
		let result = transaction
			.query_one_into::<Row>(statement.into(), db::params![user.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to get the user");
		let billing_customer_id = if let Some(billing_customer_id) = row.billing_customer_id {
			billing_customer_id
		} else {
			let statement = format!("update users set billing_customer_id = {p}1 where id = {p}2;");
			let result = transaction
				.execute(
					statement.into(),
					db::params![billing_customer_id.clone(), user.to_string()],
				)
				.await;
			crate::database::retry!(result, "failed to manage the billing customer ID");

			billing_customer_id
		};

		Ok(ControlFlow::Break(ControlFlow::Break(billing_customer_id)))
	}

	async fn manage_user_billing_primary_region(
		&self,
		mut arg: tg::user::billing::manage::Arg,
	) -> tg::Result<tg::user::billing::manage::Output> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.manage_user_billing(arg).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to manage user billing in the primary region"
			)
		})?;

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
