use {
	crate::{Server, Session, database},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub mod cancel;
pub mod children;
pub mod control;
pub mod finalize;
pub mod finish;
pub mod get;
pub mod list;
pub mod metadata;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod stdio;
pub mod store;
pub mod touch;
pub mod tty;
pub mod wait;

impl Server {
	pub(crate) async fn create_process_token(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<String> {
		let token = Self::create_process_token_string();
		let process = process.to_string();
		self.process_store
			.run(|transaction| {
				let process = process.clone();
				let token = token.clone();
				async move {
					Self::create_process_token_with_transaction(transaction, &process, &token).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the process token"))?;
		Ok(token)
	}

	async fn create_process_token_with_transaction(
		transaction: &database::Transaction<'_>,
		process: &str,
		token: &str,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_tokens (process, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![process, token];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn create_process_token_string() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn delete_sandbox_process_tokens_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from process_tokens
				where process in (
					select id
					from processes
					where sandbox = {p}1
				);
			"
		);
		let params = db::params![sandbox.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_dequeue_process_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		let id = id.to_string();
		let n = self
			.process_store
			.run(|transaction| {
				let id = id.clone();
				async move { Self::try_dequeue_process_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the process"))?;

		if n == 0 {
			return Ok(false);
		}

		let subject = format!("processes.{id}.status");
		self.messenger.publish(subject, ()).await.ok();

		Ok(true)
	}

	async fn try_dequeue_process_with_transaction(
		transaction: &database::Transaction<'_>,
		id: &str,
	) -> tg::Result<ControlFlow<u64, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set
					started_at = {p}1,
					status = 'dequeued'
				where id = {p}2 and status = 'created';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}

	pub(crate) async fn try_start_process_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		let id = id.to_string();
		let n = self
			.process_store
			.run(|transaction| {
				let id = id.clone();
				async move { Self::try_start_process_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to start the process"))?;

		if n == 0 {
			return Ok(false);
		}

		let subject = format!("processes.{id}.status");
		self.messenger.publish(subject, ()).await.ok();

		Ok(true)
	}

	async fn try_start_process_with_transaction(
		transaction: &database::Transaction<'_>,
		id: &str,
	) -> tg::Result<ControlFlow<u64, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set status = 'started'
				where id = {p}1 and status = 'dequeued';
			"
		);
		let params = db::params![id];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}
}

impl Session {
	pub(crate) fn process_permission_for_data(
		&self,
		data: &tg::process::Data,
	) -> tg::grant::permission::process::Set {
		let mut permissions = tg::grant::permission::process::Set::NODE;
		if self.process_children_grant_subtree(data.children.as_deref().unwrap_or_default()) {
			permissions.insert(tg::grant::permission::process::Set::SUBTREE);
		}
		if self
			.process_output_grants_subtree(data.output.as_ref())
			.unwrap_or(true)
		{
			permissions.insert(tg::grant::permission::process::Set::NODE_OUTPUT);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_OUTPUT);
		}
		if self.process_error_grants_subtree(data.error.as_ref()) {
			permissions.insert(tg::grant::permission::process::Set::NODE_ERROR);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_ERROR);
		}
		if self.process_log_grants_subtree(data.log.as_ref()) {
			permissions.insert(tg::grant::permission::process::Set::NODE_LOG);
			permissions.insert(tg::grant::permission::process::Set::SUBTREE_LOG);
		}
		permissions
	}

	fn process_children_grant_subtree(&self, children: &[tg::process::data::Child]) -> bool {
		children
			.iter()
			.all(|child| self.process_token_grants_subtree(&child.process))
	}

	fn process_token_grants_subtree(&self, process: &tg::MaybeWithToken<tg::process::Id>) -> bool {
		let tg::Either::Right(process) = process else {
			return false;
		};
		let resource = tg::grant::Resource::Id(process.id.clone().into());
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Subtree);
		self.authorize_token(&resource, permission.into(), &process.token)
	}

	fn process_output_grants_subtree(&self, output: Option<&tg::value::Data>) -> Option<bool> {
		output.map(|output| self.value_data_tokens_grant_subtree(output))
	}

	fn process_error_grants_subtree(
		&self,
		error: Option<&tg::Either<tg::error::Data, tg::MaybeWithToken<tg::error::Id>>>,
	) -> bool {
		let Some(error) = error else {
			return true;
		};
		match error {
			tg::Either::Left(data) => {
				let mut children = std::collections::BTreeSet::new();
				data.children(&mut children);
				children.is_empty()
			},
			tg::Either::Right(error) => self.object_token_grants_subtree_for_process(error),
		}
	}

	fn process_log_grants_subtree(&self, log: Option<&tg::MaybeWithToken<tg::blob::Id>>) -> bool {
		log.is_none_or(|log| self.object_token_grants_subtree_for_process(log))
	}

	fn value_data_tokens_grant_subtree(&self, data: &tg::value::Data) -> bool {
		match data {
			tg::value::Data::Array(array) => array
				.iter()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::value::Data::Map(map) => map
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::value::Data::Mutation(mutation) => {
				self.mutation_data_tokens_grant_subtree(mutation)
			},
			tg::value::Data::Object(object) => self.object_token_grants_subtree_for_process(object),
			tg::value::Data::Template(template) => {
				self.template_data_tokens_grant_subtree(template)
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => true,
		}
	}

	fn mutation_data_tokens_grant_subtree(&self, data: &tg::mutation::Data) -> bool {
		match data {
			tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
				values
					.iter()
					.all(|value| self.value_data_tokens_grant_subtree(value))
			},
			tg::mutation::Data::Merge { value } => value
				.values()
				.all(|value| self.value_data_tokens_grant_subtree(value)),
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				self.template_data_tokens_grant_subtree(template)
			},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.value_data_tokens_grant_subtree(value)
			},
			tg::mutation::Data::Unset => true,
		}
	}

	fn template_data_tokens_grant_subtree(&self, data: &tg::template::Data) -> bool {
		data.components.iter().all(|component| match component {
			tg::template::data::Component::Artifact(artifact) => {
				self.object_token_grants_subtree_for_process(artifact)
			},
			tg::template::data::Component::Placeholder(_)
			| tg::template::data::Component::String(_) => true,
		})
	}

	fn object_token_grants_subtree_for_process<T>(&self, object: &tg::MaybeWithToken<T>) -> bool
	where
		T: Clone + Into<tg::Id>,
	{
		let tg::Either::Right(object) = object else {
			return false;
		};
		let resource = tg::grant::Resource::Id(object.id.clone().into());
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		self.authorize_token(&resource, permission.into(), &object.token)
	}
}

impl Session {
	pub(crate) async fn get_process_exists_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		// Get a database connection.
		let mut connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		// Check if the process exists.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from processes
				where
					processes.id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let exists = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
	}
}

impl Server {
	fn spawn_publish_process_status_task(&self, id: &tg::process::Id) {
		let subject = format!("processes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the process status message");
				}
			}
		});
	}

	pub(crate) async fn try_lock_process_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<tg::process::Status>, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set stored_at = stored_at
				where id = {p}1
				returning status;
			"
		);
		let params = db::params![id.to_string()];
		let result = transaction
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await;
		let status = crate::database::retry!(result, "failed to lock the process");
		Ok(ControlFlow::Break(status.map(|status| status.0)))
	}

	pub(crate) async fn update_process_lease_count_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set lease_count = (
					select count(*)
					from process_leases
					where process = {p}1
				)
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to update the lease count");
		Ok(ControlFlow::Break(()))
	}
}
