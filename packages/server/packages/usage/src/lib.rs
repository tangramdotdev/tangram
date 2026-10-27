use {
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream::FuturesUnordered},
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::{Session, database::Transaction},
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn get_usage_regions(
		&self,
		session: &Session,
		account: &tg::usage::Account,
		period: tg::usage::Period,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<tg::usage::Output> {
		let Some(regions) = &session.server.config.regions else {
			return self.get_usage_local(session, account, period).await;
		};

		// Read each region once using an explicit region to prevent another fan-out.
		let regions = regions
			.iter()
			.map(|region| region.name.as_str())
			.collect::<BTreeSet<_>>();
		let futures = regions
			.into_iter()
			.map(|region| async move {
				if Some(region) == session.server.config.region.as_deref() {
					return self.get_usage_local(session, account, period).await;
				}
				let client = session.get_region_session(region).await.map_err(
					|error| tg::error!(!error, %region, "failed to get the region client"),
				)?;
				let mut arg = tg::usage::Arg::with_period(period);
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region.to_owned()),
				});
				arg.tokens = tokens.for_location(&location);
				arg.location = Some(location.into());
				let output = match account {
					tg::usage::Account::Organization(id) => {
						let selector = tg::organization::Selector::Id(id.clone());
						client.try_get_organization_usage(&selector, arg).await
					},
					tg::usage::Account::User(id) => {
						let selector = tg::user::Selector::Id(id.clone());
						client.try_get_user_usage(Some(&selector), arg).await
					},
				}
				.map_err(|error| tg::error!(!error, %region, "failed to get the usage"))?
				.ok_or_else(|| tg::error!(%region, "failed to find the usage account"))?;

				Ok(output)
			})
			.collect::<FuturesUnordered<_>>();
		let outputs = futures.try_collect::<Vec<_>>().await?;

		// Sum the regional usage.
		let mut aggregate = tg::usage::Aggregate::default();
		for output in outputs {
			if output.account != account.id() || output.period != period.range() {
				return Err(tg::error!(
					"the regional usage account or period does not match"
				));
			}
			let regional = tg::usage::Aggregate {
				object_count: output.object_count,
				object_size: output.object_size,
				process_count: output.process_count,
				sandbox_count: output.sandbox_count,
				sandbox_cpu_consumption: output.sandbox_cpu_consumption,
				sandbox_cpu_reservation: output.sandbox_cpu_reservation,
				sandbox_memory_consumption: output.sandbox_memory_consumption,
				sandbox_memory_reservation: output.sandbox_memory_reservation,
				sandbox_storage_consumption: output.sandbox_storage_consumption,
				sandbox_storage_reservation: output.sandbox_storage_reservation,
			};
			aggregate.checked_add(regional)?;
		}
		let output = usage_output(account, period, aggregate);

		Ok(output)
	}

	pub(crate) async fn get_usage_local(
		&self,
		session: &Session,
		account: &tg::usage::Account,
		period: tg::usage::Period,
	) -> tg::Result<tg::usage::Output> {
		if !session.server.config.usage.enabled {
			return Err(tg::error!("usage tracking is disabled"));
		}
		let now = session.server.clock.now();
		let aggregate = session.server.index.get_usage(account, period, now).await?;
		let output = usage_output(account, period, aggregate);
		Ok(output)
	}

	pub(crate) async fn usage_account_for_specifier_with_transaction(
		&self,
		session: &Session,
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::usage::Account>, tangram_server_core::database::Error>>
	{
		let prefix = specifier
			.prefixes()
			.next()
			.expect("a specifier should have a component");
		let id =
			match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
				transaction,
				&prefix,
			)
			.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let principal = match id {
			Some(id) => match id.kind() {
				tg::id::Kind::Group => Some(tg::Principal::Group(id.try_into()?)),
				tg::id::Kind::Organization => Some(tg::Principal::Organization(id.try_into()?)),
				tg::id::Kind::User => Some(tg::Principal::User(id.try_into()?)),
				_ => None,
			},
			None => None,
		};
		if let Some(principal) = principal {
			let account = match self
				.usage_account_with_transaction(session, transaction, &principal)
				.await?
			{
				ControlFlow::Break(account) => account,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if account.is_some() {
				return Ok(ControlFlow::Break(account));
			}
		}

		self.usage_account_with_transaction(session, transaction, &session.context.principal)
			.await
	}

	pub(crate) async fn usage_account_with_transaction(
		&self,
		session: &Session,
		transaction: &Transaction<'_>,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Option<tg::usage::Account>, tangram_server_core::database::Error>>
	{
		if !session.server.config.usage.enabled {
			return Ok(ControlFlow::Break(None));
		}
		let mut principal = principal.clone();
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let group = match session
						.server
						.components
						.group
						.try_get_group_with_transaction(transaction, &id)
						.await?
					{
						ControlFlow::Break(group) => group,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					let Some(group) = group else {
						return Ok(ControlFlow::Break(None));
					};
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let id = match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
						transaction,
						&specifier,
					)
					.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					let Some(id) = id else {
						return Ok(ControlFlow::Break(None));
					};
					principal = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => return Ok(ControlFlow::Break(None)),
					};
				},
				tg::Principal::Organization(id) => {
					return Ok(ControlFlow::Break(Some(tg::usage::Account::Organization(
						id,
					))));
				},
				tg::Principal::User(id) => {
					return Ok(ControlFlow::Break(Some(tg::usage::Account::User(id))));
				},
				tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
					let account = self.usage_account(session, &principal).await?;

					return Ok(ControlFlow::Break(account));
				},
				_ => return Ok(ControlFlow::Break(None)),
			}
		}
	}

	pub(crate) async fn usage_account(
		&self,
		session: &Session,
		principal: &tg::Principal,
	) -> tg::Result<Option<tg::usage::Account>> {
		if !session.server.config.usage.enabled {
			return Ok(None);
		}
		let mut principal = match principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				let Some(principal) = session
					.server
					.components
					.remote
					.try_resolve_remote_context_principal(session, principal)
					.await?
				else {
					return Ok(None);
				};
				principal
			},
			_ => principal.clone(),
		};
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let Some(group) = session.server.index.try_get_group(&id).await? else {
						return Ok(None);
					};
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let Some(id) = session
						.server
						.index
						.try_get_id_for_specifier(&specifier)
						.await?
					else {
						return Ok(None);
					};
					principal = match id.kind() {
						tg::id::Kind::Organization => tg::Principal::Organization(id.try_into()?),
						tg::id::Kind::User => tg::Principal::User(id.try_into()?),
						_ => return Ok(None),
					};
				},
				tg::Principal::Organization(id) => {
					return Ok(Some(tg::usage::Account::Organization(id)));
				},
				tg::Principal::User(id) => {
					return Ok(Some(tg::usage::Account::User(id)));
				},
				tg::Principal::Anonymous | tg::Principal::Root | tg::Principal::Runner(_) => {
					return Ok(None);
				},
				tg::Principal::Process(_) | tg::Principal::Sandbox(_) => return Ok(None),
			}
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::usage::Component for Component {
	fn get_usage_regions<'a>(
		&'a self,
		session: &'a Session,
		account: &'a tg::usage::Account,
		period: tg::usage::Period,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<tg::usage::Output>> {
		self.get_usage_regions(session, account, period, tokens)
			.boxed()
	}

	fn get_usage_local<'a>(
		&'a self,
		session: &'a Session,
		account: &'a tg::usage::Account,
		period: tg::usage::Period,
	) -> BoxFuture<'a, tg::Result<tg::usage::Output>> {
		self.get_usage_local(session, account, period).boxed()
	}

	fn usage_account_for_specifier_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a Transaction<'_>,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::usage::Account>, tangram_server_core::database::Error>>,
	> {
		self.usage_account_for_specifier_with_transaction(session, transaction, specifier)
			.boxed()
	}

	fn usage_account_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a Transaction<'_>,
		principal: &'a tg::Principal,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::usage::Account>, tangram_server_core::database::Error>>,
	> {
		self.usage_account_with_transaction(session, transaction, principal)
			.boxed()
	}

	fn usage_account<'a>(
		&'a self,
		session: &'a Session,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Account>>> {
		self.usage_account(session, principal).boxed()
	}
}

fn usage_output(
	account: &tg::usage::Account,
	period: tg::usage::Period,
	aggregate: tg::usage::Aggregate,
) -> tg::usage::Output {
	tg::usage::Output {
		account: account.id(),
		object_count: aggregate.object_count,
		object_size: aggregate.object_size,
		period: period.range(),
		process_count: aggregate.process_count,
		sandbox_count: aggregate.sandbox_count,
		sandbox_cpu_consumption: aggregate.sandbox_cpu_consumption,
		sandbox_cpu_reservation: aggregate.sandbox_cpu_reservation,
		sandbox_memory_consumption: aggregate.sandbox_memory_consumption,
		sandbox_memory_reservation: aggregate.sandbox_memory_reservation,
		sandbox_storage_consumption: aggregate.sandbox_storage_consumption,
		sandbox_storage_reservation: aggregate.sandbox_storage_reservation,
	}
}
