use crate::Server;
use indoc::indoc;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn indexer_task(&self) -> tg::Result<()> {
		loop {
			// Get a database connection
			let options = db::ConnectionOptions {
				kind: db::ConnectionKind::Write,
				priority: db::Priority::Low,
			};
			let connection = match self.database.connection_with_options(options).await {
				Ok(connection) => connection,
				Err(error) => {
					tracing::error!(?error, "failed to get a database connection");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};
			let statement = indoc!(
				"
					with updates as (
						select
							objects.id as id,
							coalesce(min(child_objects.complete), 1) as complete,
							1 + coalesce(sum(child_objects.count), 0) as count,
							1 + coalesce(max(child_objects.depth), 0) as depth,
							objects.size + coalesce(sum(child_objects.weight), 0) as weight
						from objects
						left join object_children on object_children.object = objects.id
						left join objects child_objects on child_objects.id = object_children.child
						where objects.complete = 0 and objects.incomplete_children = 0
						group by objects.id
						limit 1000
					)
					update objects
					set
						complete = updates.complete,
						count = updates.count,
						depth = updates.depth,
						weight = updates.weight
					from updates
					where objects.id = updates.id;
				"
			);
			let params = db::params![];
			match connection.execute(statement.into(), params).await {
				Ok(0) => {
					drop(connection);
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => (),
				Err(error) => {
					drop(connection);
					tracing::error!(?error, "failed to index the objects");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			};
		}
	}
}
