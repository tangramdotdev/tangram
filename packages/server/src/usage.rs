use {
	crate::{Session, database::Transaction},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn get_usage_regions(
		&self,
		account: &tg::usage::Account,
		period: tg::usage::Period,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<tg::usage::Output> {
		let Some(regions) = &self.server.config.regions else {
			return self.get_usage_local(account, period).await;
		};

		// Read each region once using an explicit region to prevent another fan-out.
		let regions = regions
			.iter()
			.map(|region| region.name.as_str())
			.collect::<BTreeSet<_>>();
		let futures = regions
			.into_iter()
			.map(|region| async move {
				if Some(region) == self.server.config.region.as_deref() {
					return self.get_usage_local(account, period).await;
				}
				let client = self.get_region_session(region).await.map_err(
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
						client.try_get_user_usage(&selector, arg).await
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
				sandbox_cpu: output.sandbox_cpu,
				sandbox_memory: output.sandbox_memory,
			};
			aggregate.checked_add(regional)?;
		}
		let output = usage_output(account, period, aggregate);

		Ok(output)
	}

	pub(crate) async fn get_usage_local(
		&self,
		account: &tg::usage::Account,
		period: tg::usage::Period,
	) -> tg::Result<tg::usage::Output> {
		if !self.server.config.usage.enabled {
			return Err(tg::error!("usage tracking is disabled"));
		}
		let now = self.server.clock.now()?;
		let aggregate = self.server.index.get_usage(account, period, now).await?;
		let output = usage_output(account, period, aggregate);
		Ok(output)
	}

	pub(crate) async fn usage_account_for_specifier_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::usage::Account>, crate::database::Error>> {
		let prefix = specifier
			.prefixes()
			.next()
			.expect("a specifier should have a component");
		let id = match Self::try_get_id_for_specifier_with_transaction(transaction, &prefix).await?
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
				.usage_account_with_transaction(transaction, &principal)
				.await?
			{
				ControlFlow::Break(account) => account,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if account.is_some() {
				return Ok(ControlFlow::Break(account));
			}
		}

		self.usage_account_with_transaction(transaction, &self.context.principal)
			.await
	}

	pub(crate) async fn usage_account_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Option<tg::usage::Account>, crate::database::Error>> {
		if !self.server.config.usage.enabled {
			return Ok(ControlFlow::Break(None));
		}
		let mut principal = principal.clone();
		loop {
			match principal {
				tg::Principal::Group(id) => {
					let group = match Self::try_get_group_with_transaction(transaction, &id).await?
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
					let id = match Self::try_get_id_for_specifier_with_transaction(
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
					let account = self.usage_account(&principal).await?;

					return Ok(ControlFlow::Break(account));
				},
				_ => return Ok(ControlFlow::Break(None)),
			}
		}
	}

	pub(crate) async fn usage_account(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Option<tg::usage::Account>> {
		if !self.server.config.usage.enabled {
			return Ok(None);
		}
		let mut principal = match principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				let Some(principal) = self.try_resolve_remote_context_principal(principal).await?
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
					let Some(group) = self.server.index.try_get_group(&id).await? else {
						return Ok(None);
					};
					let specifier = group
						.specifier
						.prefixes()
						.next()
						.expect("a specifier should have a component");
					let Some(id) = self
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
		sandbox_cpu: aggregate.sandbox_cpu,
		sandbox_memory: aggregate.sandbox_memory,
	}
}
