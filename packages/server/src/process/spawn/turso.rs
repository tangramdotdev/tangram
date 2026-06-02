use {
	crate::Session,
	indoc::indoc,
	std::{collections::HashSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn add_process_child_creates_cycle_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<bool, db::turso::Error>> {
		let child = child.to_string();
		let mut queue = vec![parent.to_string()];
		let mut visited = HashSet::new();
		while let Some(current) = queue.pop() {
			if current == child {
				return Ok(ControlFlow::Break(true));
			}
			if !visited.insert(current.clone()) {
				continue;
			}
			#[derive(db::row::Deserialize)]
			struct Row {
				process: String,
			}
			let statement = indoc!(
				"
					select process
					from process_children
					where child = ?1;
				"
			);
			let result = transaction
				.query_all_into(statement.into(), db::params![current])
				.await;
			let parents: Vec<Row> =
				crate::database::retry!(result, "failed to execute the cycle check");
			queue.extend(parents.into_iter().map(|row| row.process));
		}
		Ok(ControlFlow::Break(false))
	}

	pub(super) async fn find_add_process_child_cycle_path_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<String>, db::turso::Error>> {
		struct Frame {
			children: Vec<String>,
			next_child: usize,
		}

		let parent = parent.to_string();
		let mut path = vec![child.to_string()];
		let mut stack = vec![Frame {
			children: match Self::query_process_children_turso_with_transaction(
				transaction,
				&child.to_string(),
			)
			.await
			{
				Ok(ControlFlow::Break(children)) => children,
				Ok(ControlFlow::Continue(error)) => return Ok(ControlFlow::Continue(error)),
				Err(error) => {
					tracing::error!(?error, "failed to get the cycle");
					return Ok(ControlFlow::Break(None));
				},
			},
			next_child: 0,
		}];
		loop {
			if path.last().is_some_and(|current| current == &parent) {
				return Ok(ControlFlow::Break(Some(format!(
					"{parent} {}",
					path.join(" ")
				))));
			}
			let Some(frame) = stack.last_mut() else {
				return Ok(ControlFlow::Break(None));
			};
			if frame.next_child >= frame.children.len() {
				stack.pop();
				path.pop();
				continue;
			}
			let next = frame.children[frame.next_child].clone();
			frame.next_child += 1;
			if path.contains(&next) {
				continue;
			}
			path.push(next.clone());
			let children =
				match Self::query_process_children_turso_with_transaction(transaction, &next).await
				{
					Ok(ControlFlow::Break(children)) => children,
					Ok(ControlFlow::Continue(error)) => return Ok(ControlFlow::Continue(error)),
					Err(error) => {
						tracing::error!(?error, "failed to get the cycle");
						return Ok(ControlFlow::Break(None));
					},
				};
			stack.push(Frame {
				children,
				next_child: 0,
			});
		}
	}

	async fn query_process_children_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		process: &str,
	) -> tg::Result<ControlFlow<Vec<String>, db::turso::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			child: String,
		}
		let statement = indoc!(
			"
				select child
				from process_children
				where process = ?1;
			"
		);
		let result = transaction
			.query_all_into(statement.into(), db::params![process.to_owned()])
			.await;
		let children: Vec<Row> = crate::database::retry!(result, "failed to get the cycle");
		Ok(ControlFlow::Break(
			children.into_iter().map(|row| row.child).collect(),
		))
	}

	pub(super) async fn update_parent_depths_turso(
		&self,
		transaction: &db::turso::Transaction<'_>,
		child_ids: Vec<String>,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		let mut current_ids = child_ids;

		while !current_ids.is_empty() {
			let mut updated_ids = Vec::new();

			for child_id in &current_ids {
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
				let result = transaction
					.query_all_into::<Parent>(statement.into(), params)
					.await;
				let parents: Vec<Parent> =
					crate::database::retry!(result, "failed to query parent depths");

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
						let result = transaction.execute(statement.into(), params).await;
						let rows = crate::database::retry!(result, "failed to update parent depth");

						if rows > 0 {
							updated_ids.push(parent.process);
						}
					}
				}
			}

			if updated_ids.is_empty() {
				break;
			}

			current_ids = updated_ids;
		}

		Ok(ControlFlow::Break(()))
	}
}
