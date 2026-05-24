use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn post_tag_batch_postgres(
		&self,
		database: &db::postgres::Database,
		arg: &tg::tag::batch::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: &[bool],
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		// Begin a transaction.
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

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
			Self::put_tag_postgres_inner(
				&mut transaction,
				tag,
				&arg,
				created_by,
				*grant_creator_admin,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to perform the transaction"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		Ok(())
	}
}
