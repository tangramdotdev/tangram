use crate::Server;
use indoc::formatdoc;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;

impl Server {
	pub async fn index(&self) -> tg::Result<()> {
		let config = self.config.indexer.clone().unwrap_or_default();
		loop {
			let n = self.indexer_task_inner(&config).await?;
			if n == 0 {
				break;
			}
		}
		Ok(())
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		loop {
			let result = self.indexer_task_inner(config).await;
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => (),
				Err(error) => {
					tracing::error!(?error, "failed to index the objects");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub(crate) async fn indexer_task_inner(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let locked = match connection {
			Either::Left(_) => "",
			Either::Right(_) => "for update skip locked",
		};
		let statement = formatdoc!(
			"
				with update_objects as (
					select id, size
					from objects
					where objects.complete = 0 and objects.incomplete_children = 0
					{locked}
				),
				updates as (
					select
						update_objects.id as id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						update_objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from update_objects
					left join object_children on object_children.object = update_objects.id
					left join objects child_objects on child_objects.id = object_children.child
					group by update_objects.id, update_objects.size
					limit {p}1
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
		let batch_size = config.batch_size;
		let params = db::params![batch_size];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		Ok(n)
	}
}
