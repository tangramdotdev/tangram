use {
	crate::Session,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, Error as _},
};

impl Session {
	pub(crate) async fn post_tag_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: &tg::tag::batch::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: &[bool],
	) -> tg::Result<()> {
		let arg = arg.clone();
		let created_by = created_by.cloned();
		let grant_creator_admin = grant_creator_admin.to_vec();
		database
			.run(move |transaction, cache| {
				for (tg::tag::batch::Item { tag, item, force }, grant_creator_admin) in
					arg.tags.iter().zip(&grant_creator_admin)
				{
					let arg = tg::tag::put::Arg {
						force: *force,
						item: item.clone(),
						location: None,
						all: false,
						replicate: false,
						tag: None,
					};
					let result = Self::put_tag_sqlite_sync(
						transaction,
						cache,
						tag,
						&arg,
						created_by.as_ref(),
						*grant_creator_admin,
					);
					match result {
						Ok(ControlFlow::Break(())) => {},
						Ok(ControlFlow::Continue(error)) => {
							return Ok::<ControlFlow<(), db::sqlite::Error>, db::sqlite::Error>(
								ControlFlow::Continue(error),
							);
						},
						Err(error) => return Err(db::sqlite::Error::other(error)),
					}
				}
				Ok::<ControlFlow<(), db::sqlite::Error>, db::sqlite::Error>(ControlFlow::Break(()))
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put tags"))
	}
}
