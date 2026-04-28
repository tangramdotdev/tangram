use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn update_parent_depths_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		child_ids: Vec<String>,
	) -> tg::Result<()> {
		let mut current_ids = child_ids;

		while !current_ids.is_empty() {
			let mut updated_ids = Vec::new();

			// Process each child to find and update its parents.
			for child_id in &current_ids {
				// Find parents of this child and their max child depth.
				#[derive(db::row::Deserialize)]
				struct Parent {
					process: String,
					max_child_depth: Option<i64>,
				}
				let statement = indoc!(
					"
						select process_children.process, max(processes.depth) as max_child_depth
						from process_children
						join processes on processes.id = process_children.child
						where process_children.child = ?1
						group by process_children.process;
					"
				);
				let params = db::params![child_id.clone()];
				let parents: Vec<Parent> = transaction
					.query_all_into::<Parent>(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to query parent depths"))?;

				// Update each parent's depth if needed.
				for parent in parents {
					if let Some(max_child_depth) = parent.max_child_depth {
						let statement = indoc!(
							"
								update processes
								set depth = max(depth, ?1)
								where id = ?2 and depth < ?1;
							"
						);
						let new_depth = max_child_depth + 1;
						let params = db::params![new_depth, parent.process.clone()];
						let rows = transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to update parent depth")
							})?;

						// If we updated this parent, track it for next iteration.
						if rows > 0 {
							updated_ids.push(parent.process);
						}
					}
				}
			}

			// Exit if no parents were updated.
			if updated_ids.is_empty() {
				break;
			}

			// Continue with the updated parents.
			current_ids = updated_ids;
		}

		Ok(())
	}
}
