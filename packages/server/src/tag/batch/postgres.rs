use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn post_tag_batch_postgres(
		&self,
		database: &db::postgres::Database,
		arg: &tg::tag::batch::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: &[bool],
	) -> tg::Result<()> {
		let arg = arg.clone();
		let created_by = created_by.cloned();
		let grant_creator_admin = grant_creator_admin.to_vec();
		database
			.run(|transaction| {
				let arg = arg.clone();
				let created_by = created_by.clone();
				let grant_creator_admin = grant_creator_admin.clone();
				async move {
					Self::post_tag_batch_postgres_with_transaction(
						transaction,
						&arg,
						created_by.as_ref(),
						&grant_creator_admin,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put tags"))
	}

	async fn post_tag_batch_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		arg: &tg::tag::batch::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: &[bool],
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		for (tg::tag::batch::Item { tag, item, force }, grant_creator_admin) in
			arg.tags.iter().zip(grant_creator_admin)
		{
			let arg = tg::tag::put::Arg {
				force: *force,
				item: item.clone(),
				location: None,
				all: false,
				replicate: false,
				tag: None,
			};
			match Self::put_tag_postgres_inner(
				transaction,
				tag,
				&arg,
				created_by,
				*grant_creator_admin,
			)
			.await?
			{
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		Ok(ControlFlow::Break(()))
	}
}
