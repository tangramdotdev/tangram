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
};

impl Session {
	pub(crate) async fn manage_organization_billing(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::billing::manage::Arg,
	) -> tg::Result<tg::organization::billing::manage::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.manage_organization_billing_local(organization).await,
			tg::Location::Remote(remote) => {
				self.manage_organization_billing_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn manage_organization_billing_local(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<tg::organization::billing::manage::Output> {
		// Authorize the organization.
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		match self.authorize(organization.clone(), permission).await? {
			None => return Err(tg::error!("failed to find the organization")),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}

		// Get the billing provider.
		let stripe = self
			.server
			.billing
			.clone()
			.ok_or_else(|| tg::error!("billing is not configured"))?;

		// Get or create the Stripe customer.
		let organization = organization.clone();
		let stripe_customer_id = self
			.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let stripe = stripe.clone();
				async move {
					let id = match &organization {
						tg::Selector::Id(id) => Some(id.clone()),
						tg::Selector::Specifier(specifier) => {
							Session::try_get_id_for_specifier_with_transaction(
								transaction,
								specifier,
							)
							.await?
							.and_then(|id| id.try_into().ok())
						},
					}
					.ok_or_else(|| tg::error!("failed to find the organization"))?;

					#[derive(db::row::Deserialize)]
					struct Row {
						name: String,
						stripe_customer_id: Option<String>,
					}

					let p = transaction.p();
					let statement = formatdoc!(
						"
							select organizations.name, organizations.stripe_customer_id
							from organizations
							where organizations.id = {p}1;
						"
					);
					let row = transaction
						.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
						.await
						.map_err(|error| tg::error!(!error, "failed to get the organization"))?;
					let stripe_customer_id =
						if let Some(stripe_customer_id) = row.stripe_customer_id {
							stripe_customer_id
						} else {
							let metadata = BTreeMap::from([(
								"tangram_organization_id".to_owned(),
								id.to_string(),
							)]);
							let stripe_customer_id = stripe
								.create_customer(CreateCustomerArg {
									email: None,
									idempotency_key: format!("tangram-organization-{id}"),
									metadata,
									name: row.name,
								})
								.await?;

							let statement = format!(
								"update organizations set stripe_customer_id = {p}1 where id = {p}2;"
							);
							transaction
								.execute(
									statement.into(),
									db::params![stripe_customer_id.clone(), id.to_string()],
								)
								.await
								.map_err(|error| {
									tg::error!(!error, "failed to manage the Stripe customer ID")
								})?;

							stripe_customer_id
						};

					Ok::<_, crate::database::Error>(ControlFlow::Break(stripe_customer_id))
				}
				.boxed()
			})
			.await?;

		// Create the Stripe portal session.
		let url = stripe
			.create_payment_method_update(&stripe_customer_id)
			.await?;
		let output = tg::organization::billing::manage::Output { url };

		Ok(output)
	}

	async fn manage_organization_billing_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::billing::manage::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::organization::billing::manage::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client
			.manage_organization_billing(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to manage the organization billing"),
			)?;

		Ok(output)
	}

	pub(crate) async fn manage_organization_billing_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let organization = organization.replace(':', "/").parse()?;
		let output = self.manage_organization_billing(&organization, arg).await?;
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
