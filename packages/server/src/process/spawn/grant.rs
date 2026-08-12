use {crate::Session, tangram_client::prelude::*, tangram_database::prelude::*};

impl Session {
	pub(super) async fn spawn_process_create_public_grant_if_requested(
		&self,
		arg: &tg::process::spawn::Arg,
		output: &tg::process::spawn::Output,
	) -> tg::Result<()> {
		if arg.public
			&& let tg::Either::Right(id) = &output.process
		{
			self.spawn_process_create_public_grant(id).await?;
		}
		Ok(())
	}

	async fn spawn_process_create_public_grant(&self, id: &tg::process::Id) -> tg::Result<()> {
		let resource = tg::Id::from(id.clone());
		let existing =
			{
				let mut connection =
					self.server.database.connection().await.map_err(|error| {
						tg::error!(!error, "failed to get a database connection")
					})?;
				let transaction = connection
					.transaction()
					.await
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				Self::list_resource_grants_with_transaction(&transaction, &resource).await?
			};
		let mut covered = tg::authorization::permission::process::Set::empty();
		for grant in existing {
			if grant.subject == tg::authorization::Subject::Public
				&& let tg::authorization::permission::Set::Process(set) = grant.permissions
			{
				covered.insert(set);
			}
		}

		let mut missing = tg::authorization::permission::process::Set::empty();
		for permission in [
			tg::authorization::permission::process::Permission::Subtree,
			tg::authorization::permission::process::Permission::SubtreeCommand,
			tg::authorization::permission::process::Permission::SubtreeError,
			tg::authorization::permission::process::Permission::SubtreeLog,
			tg::authorization::permission::process::Permission::SubtreeOutput,
		] {
			let set = tg::authorization::permission::process::Set::from_permission(permission);
			if !covered.contains(set) {
				missing.insert(set);
			}
		}
		if !missing.is_empty() {
			self.create_grant(tg::grant::create::Arg {
				subject: tg::authorization::subject::Selector::Subject(
					tg::authorization::Subject::Public,
				),
				permissions: tg::Either::Left(tg::authorization::permission::Set::Process(missing)),
				resource: tg::Referent::with_node(tg::Selector::Id(resource)),
			})
			.await?;
		}
		Ok(())
	}
}
