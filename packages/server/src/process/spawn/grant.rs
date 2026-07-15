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
		let mut covered = tg::grant::permission::process::Set::empty();
		for grant in existing {
			if grant.principal == tg::grant::Principal::Public
				&& let tg::grant::permission::Set::Process(set) = grant.permissions
			{
				covered.insert(set);
			}
		}

		let mut missing = tg::grant::permission::process::Set::empty();
		for permission in [
			tg::grant::permission::process::Permission::Subtree,
			tg::grant::permission::process::Permission::SubtreeCommand,
			tg::grant::permission::process::Permission::SubtreeError,
			tg::grant::permission::process::Permission::SubtreeLog,
			tg::grant::permission::process::Permission::SubtreeOutput,
		] {
			let set = tg::grant::permission::process::Set::from_permission(permission);
			if !covered.contains(set) {
				missing.insert(set);
			}
		}
		if !missing.is_empty() {
			self.create_grant(tg::grant::create::Arg {
				principal: tg::principal::Selector::Principal(tg::grant::Principal::Public),
				permissions: tg::Either::Left(tg::grant::permission::Set::Process(missing)),
				resource: tg::grant::Resource::Id(resource),
			})
			.await?;
		}
		Ok(())
	}
}
