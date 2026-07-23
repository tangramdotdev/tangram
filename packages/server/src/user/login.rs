use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
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
		// Finish the login.
		let current = self.clone();
		let user = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let current = current.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let user = current
						.upsert_login_user_with_transaction(transaction, &arg, &mut batch)
						.await?;
					for email in &arg.emails {
						let p = transaction.p();
						let statement = formatdoc!(
							r#"
								insert into user_emails ("user", email)
								values ({p}1, {p}2)
								on conflict ("user", email) do nothing;
							"#
						);
						transaction
							.execute(
								statement.into(),
								db::params![user.id.to_string(), email.clone()],
							)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to execute the statement")
							})?;
					}
					let token = create_token();
					let p = transaction.p();
					let statement = formatdoc!(
						r#"
							insert into user_tokens (token, "user")
							values ({p}1, {p}2);
						"#
					);
					transaction
						.execute(
							statement.into(),
							db::params![token.clone(), user.id.to_string()],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					let statement = formatdoc!(
						r#"
							update logins
							set status = 'finished', "user" = {p}1, token = {p}2, updated_at = {p}3
							where code = {p}4 and status = 'started';
						"#
					);
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					transaction
						.execute(
							statement.into(),
							db::params![user.id.to_string(), token, now, arg.code.clone(),],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					let node = Self::try_get_node_by_id_with_transaction(
						transaction,
						&user.id.clone().into(),
					)
					.await?
					.unwrap();
					let user = Self::user_from_node_with_transaction(transaction, node).await?;
					current
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(user))
				}
				.boxed()
			})
			.await?;

		Ok(user)
	}

	async fn upsert_login_user_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &FinishLoginArg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::User> {
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
			if let Some(row) = transaction
				.query_optional_into::<Row>(
					statement.into(),
					db::params![identity.provider.clone(), identity.subject.clone()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			{
				let id = row.user.parse::<tg::user::Id>()?;
				let node = Self::try_get_node_by_id_with_transaction(transaction, &id.into())
					.await?
					.ok_or_else(|| tg::error!("invalid user identity"))?;
				return Self::user_from_node_with_transaction(transaction, node).await;
			}
		}

		// Get the login.
		#[derive(db::row::Deserialize)]
		struct LoginRow {
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
		let login = transaction
			.query_one_into::<LoginRow>(statement.into(), db::params![arg.code.clone()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		// Get the specifier.
		let specifier = login
			.name
			.as_deref()
			.map(str::parse)
			.transpose()?
			.unwrap_or_else(|| arg.default_specifier.clone());
		if specifier.components().count() != 1 {
			return Err(tg::error!("invalid user specifier"));
		}

		// Get or create the user.
		let user = if let Some(node) =
			Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
		{
			if node.kind != tg::id::Kind::User {
				return Err(tg::error!("specifier is already in use"));
			}
			Self::user_from_node_with_transaction(transaction, node).await?
		} else {
			let id = tg::user::Id::new();
			let node = Self::create_node_with_transaction(
				transaction,
				&id.clone().into(),
				tg::id::Kind::User,
				&specifier,
				None,
			)
			.await?;
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into users (id, name)
					values ({p}1, {p}2);
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![id.to_string(), node.name.clone()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			batch.items.push(tangram_index::batch::Item::PutUser(
				tangram_index::user::put::Arg {
					id: id.clone(),
					specifier: node.specifier.clone(),
				},
			));
			Self::user_from_node_with_transaction(transaction, node).await?
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
			transaction
				.execute(
					statement.into(),
					db::params![
						identity.provider.clone(),
						identity.subject.clone(),
						user.id.to_string()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}

		Ok(user)
	}
}

pub(super) fn create_token() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}

pub(super) fn create_code() -> String {
	let bytes = rand::random::<[u8; 5]>();
	tg::id::ENCODING.encode(&bytes)
}
