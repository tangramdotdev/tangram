use {
	crate::Session,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn post_tag_batch_turso(
		&self,
		database: &db::turso::Database,
		arg: &tg::tag::batch::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: &[bool],
	) -> tg::Result<()> {
		db::turso::run!(database, |transaction| {
			Ok::<_, tg::Error>({
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
					let result = Self::put_tag_turso_with_transaction(
						transaction,
						tag,
						&arg,
						created_by,
						*grant_creator_admin,
					)
					.await?;
					match result {
						ControlFlow::Break(()) => {},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				}
				ControlFlow::Break(())
			})
		})
		.map_err(|error| tg::error!(!error, "failed to post the tag batch"))
	}
}
