use {super::Store, crate::GrantArg, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub(super) async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		self.put_grant(&arg.id, arg.principal, arg.subtree, arg.created_at)
			.await
	}

	pub(super) async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| async move {
			self.put_grant(&arg.id, arg.principal, arg.subtree, arg.created_at)
				.await
		}))
		.await?;
		Ok(())
	}

	pub(super) async fn put_grant(
		&self,
		id: &tg::object::Id,
		principal: tg::Principal,
		subtree: bool,
		created_at: i64,
	) -> tg::Result<()> {
		let ttl = self.grant_ttl.to_i32().unwrap();
		let expires_at = created_at + self.grant_ttl.to_i64().unwrap();
		let params = (
			id.to_bytes().to_vec(),
			principal.to_string(),
			subtree,
			created_at,
			expires_at,
			ttl,
		);
		self.session
			.execute_unpaged(&self.statements.put_object_grant, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}
}
