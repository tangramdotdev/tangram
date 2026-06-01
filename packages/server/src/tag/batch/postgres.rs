use {
	crate::Session,
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
		db::postgres::run!(database, |transaction| {
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
					ControlFlow::Continue(error) => {
						return Ok::<ControlFlow<(), db::postgres::Error>, tg::Error>(
							ControlFlow::Continue(error),
						);
					},
				}
			}
			Ok::<ControlFlow<(), db::postgres::Error>, tg::Error>(ControlFlow::Break(()))
		})
		.map_err(|error| tg::error!(!error, "failed to put tags"))
	}
}
