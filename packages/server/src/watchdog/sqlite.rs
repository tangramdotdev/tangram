use {
	crate::Server,
	indoc::formatdoc,
	std::{collections::BTreeMap, fmt::Write},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn get_process_cycles_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		messages: &[crate::process::queue::Message],
	) -> tg::Result<Vec<(tg::process::Id, tg::error::Data)>> {
		let mut results = Vec::new();
		let p = transaction.p();

		for message in messages {
			let Some(parent) = &message.parent else {
				continue;
			};
			let child = &message.process;
			let params = db::params![parent.to_string(), child.to_string()];
			let statement = formatdoc!(
				"
					with recursive ancestors as (
						select {p}1 as id
						union
						select process_children.process as id
						from ancestors
						join process_children on ancestors.id = process_children.child
					)
					select exists(
						select 1 from ancestors where id = {p}2
					);
				"
			);
			let cycle = transaction
				.query_one_value_into::<bool>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the cycle check"))?;

			if !cycle {
				continue;
			}

			// Construct a good error message by collecting the entire cycle.
			let mut message = String::from("adding this child process creates a cycle");

			// Try to reconstruct the cycle path by walking from the child through its
			// descendants until we find a path back to the parent.
			let statement = formatdoc!(
				"
					with recursive reachable (current_process, path) as (
						select {p}2, {p}2

						union

						select pc.child, r.path || ' ' || pc.child
						from reachable r
						join process_children pc on r.current_process = pc.process
						where r.path not like '%' || pc.child || '%'
					)
					select
						{p}1 || ' ' || path as cycle
					from reachable
					where current_process = {p}1
					limit 1;
				"
			);
			let params = db::params![parent.to_string(), child.to_string()];
			let cycle = transaction
				.query_one_value_into::<String>(statement.into(), params)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to get the cycle"))
				.ok();

			// Format the error message.
			if let Some(cycle) = cycle {
				let processes = cycle.split(' ').collect::<Vec<_>>();
				for i in 0..processes.len() - 1 {
					let parent = processes[i];
					let child = processes[i + 1];
					if i == 0 {
						write!(&mut message, "\n{parent} tried to add child {child}").unwrap();
					} else {
						write!(&mut message, "\n{parent} has child {child}").unwrap();
					}
				}
			}
			let error = tg::error::Data {
				code: None,
				diagnostics: None,
				location: None,
				message: Some(message),
				source: None,
				stack: None,
				values: BTreeMap::new(),
			};
			results.push((child.clone(), error));
		}

		Ok(results)
	}
}
