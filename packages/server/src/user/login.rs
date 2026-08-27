use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub mod create;
pub mod wait;

#[derive(Clone)]
pub(super) struct FinishLoginArg {
	pub code: String,
	pub default_specifier: tg::Specifier,
	pub emails: Vec<String>,
	pub identity: Option<LoginIdentity>,
}

#[derive(Clone)]
pub(super) struct LoginIdentity {
	pub provider: String,
	pub subject: String,
}

impl Session {
	pub(super) async fn finish_login(&self, arg: FinishLoginArg) -> tg::Result<tg::User> {
		let session = self.clone();
		let now = self.server.clock.unix_timestamp()?;
		let options = tangram_futures::retry::Options::default();
		let user = tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let session = session.clone();
			async move {
				match session.finish_login_attempt(&arg, now).await? {
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_index_outbox_notification_task();

		Ok(user)
	}

	async fn finish_login_attempt(
		&self,
		arg: &FinishLoginArg,
		now: i64,
	) -> tg::Result<ControlFlow<tg::User>> {
		let specifier = self.login_user_specifier(arg).await?;
		let ids_by_specifier = self
			.try_get_ids_and_ancestors_for_specifiers(std::slice::from_ref(&specifier))
			.await?;
		let session = self.clone();
		let user = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let session = session.clone();
				async move {
					session
						.finish_login_with_transaction(transaction, &arg, &ids_by_specifier, now)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(user)
	}

	async fn login_user_specifier(&self, arg: &FinishLoginArg) -> tg::Result<tg::Specifier> {
		let arg = arg.clone();
		let specifier = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let arg = arg.clone();
				async move { Self::login_user_specifier_with_transaction(transaction, &arg).await }
					.boxed()
			})
			.await?;

		Ok(specifier)
	}

	async fn login_user_specifier_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		arg: &FinishLoginArg,
	) -> tg::Result<ControlFlow<tg::Specifier, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: Option<String>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from logins
				where code = {p}1;
			"
		);
		let result = transaction
			.query_one_into::<Row>(statement.into(), db::params![arg.code.clone()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let specifier = row
			.name
			.as_deref()
			.map(str::parse)
			.transpose()?
			.unwrap_or_else(|| arg.default_specifier.clone());
		if specifier.components().count() != 1 {
			return Err(tg::error!("invalid user specifier"));
		}

		Ok(ControlFlow::Break(specifier))
	}

	async fn finish_login_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &FinishLoginArg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
		now: i64,
	) -> tg::Result<ControlFlow<ControlFlow<tg::User>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
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
		let mut batch = tangram_index::batch::Arg::default();
		let user = match self
			.upsert_login_user_with_transaction(transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(user) => user,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for email in &arg.emails {
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					insert into user_emails ("user", email)
					values ({p}1, {p}2)
					on conflict ("user", email) do nothing;
				"#
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![user.id.to_string(), email.clone()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}
		let (token_id, token) = crate::token::create();
		let token_hash = crate::token::hash(&token);
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into user_tokens (created_at, id, token, "user")
				values ({p}1, {p}2, {p}3, {p}4);
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![now, token_id.to_string(), token_hash, user.id.to_string()],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		let statement = formatdoc!(
			r#"
				update logins
				set status = 'finished', "user" = {p}1, token = {p}2, updated_at = {p}3
				where code = {p}4 and status = 'started';
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![user.id.to_string(), token, now, arg.code.clone()],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		let user = match Self::try_get_user_with_transaction(transaction, &user.id).await? {
			ControlFlow::Break(user) => user,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		.ok_or_else(|| tg::error!("failed to find the user"))?;
		match self
			.server
			.enqueue_database_index_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(ControlFlow::Break(user)))
	}

	async fn upsert_login_user_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &FinishLoginArg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::User, crate::database::Error>> {
		// Get the user from the identity.
		if let Some(identity) = &arg.identity {
			#[derive(db::row::Deserialize)]
			struct Row {
				user: String,
			}
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					select "user"
					from user_identities
					where provider = {p}1 and subject = {p}2;
				"#
			);
			let result = transaction
				.query_optional_into::<Row>(
					statement.into(),
					db::params![identity.provider.clone(), identity.subject.clone()],
				)
				.await;
			let row = crate::database::retry!(result, "failed to execute the statement");
			if let Some(row) = row {
				let id = row.user.parse::<tg::user::Id>()?;
				let user = match Self::try_get_user_with_transaction(transaction, &id).await? {
					ControlFlow::Break(user) => user,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
				.ok_or_else(|| tg::error!("invalid user identity"))?;

				return Ok(ControlFlow::Break(user));
			}
		}

		// Get the specifier.
		let specifier = match Self::login_user_specifier_with_transaction(transaction, arg).await? {
			ControlFlow::Break(specifier) => specifier,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		// Get or create the user.
		let id =
			match Self::try_get_id_for_specifier_with_transaction(transaction, &specifier).await? {
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let user = if let Some(id) = id {
			let Ok(id) = id.try_into() else {
				return Err(tg::error!("specifier is already in use"));
			};
			match Self::try_get_user_with_transaction(transaction, &id).await? {
				ControlFlow::Break(user) => user,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the user"))?
		} else {
			let id = tg::user::Id::new();
			match Self::insert_specifier_with_transaction(
				transaction,
				&id.clone().into(),
				&specifier,
			)
			.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			let name = specifier.name().to_owned();
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into users (id, name)
					values ({p}1, {p}2);
				"
			);
			let result = transaction
				.execute(statement.into(), db::params![id.to_string(), name])
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			batch.items.push(tangram_index::batch::Item::PutUser(
				tangram_index::user::put::Arg {
					billing: Some(false),
					id: id.clone(),
					specifier: specifier.clone(),
				},
			));
			match Self::try_get_user_with_transaction(transaction, &id).await? {
				ControlFlow::Break(user) => user,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the user"))?
		};

		// Insert the identity.
		if let Some(identity) = &arg.identity {
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					insert into user_identities (provider, subject, "user")
					values ({p}1, {p}2, {p}3);
				"#
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						identity.provider.clone(),
						identity.subject.clone(),
						user.id.to_string()
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(user))
	}
}

pub(super) fn create_token() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}

pub(super) fn create_code() -> String {
	let bytes = rand::random::<[u8; 5]>();
	tg::id::ENCODING.encode(&bytes)
}
