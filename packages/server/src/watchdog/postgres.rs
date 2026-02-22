use {
	crate::Server,
	std::{collections::BTreeMap, fmt::Write},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn get_process_cycles_postgres(
		transaction: &db::postgres::Transaction<'_>,
		messages: &[crate::process::queue::Message],
	) -> tg::Result<Vec<(tg::process::Id, tg::error::Data)>> {
		// Build the parameter arrays dynamically.
		let mut parent_placeholders = Vec::new();
		let mut child_placeholders = Vec::new();
		let mut params = Vec::new();
		let mut index = 1;
		for message in messages {
			let Some(parent) = &message.parent else {
				continue;
			};
			parent_placeholders.push(format!("${index}"));
			index += 1;
			child_placeholders.push(format!("${index}"));
			index += 1;
			params.push(db::Value::Text(parent.to_string()));
			params.push(db::Value::Text(message.process.to_string()));
		}
		if parent_placeholders.is_empty() {
			return Ok(Vec::new());
		}

		// Call the stored function with a single query.
		#[derive(db::row::Deserialize)]
		struct Row {
			process_id: String,
			cycle_path: Option<String>,
		}
		let statement = format!(
			"select * from get_process_cycles(array[{}]::text[], array[{}]::text[]);",
			parent_placeholders.join(", "),
			child_placeholders.join(", "),
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process cycles"))?;

		// Build the results with formatted error messages.
		let mut results = Vec::new();
		for row in rows {
			let id: tg::process::Id = row
				.process_id
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
			let mut message = String::from("adding this child process creates a cycle");
			if let Some(cycle) = row.cycle_path {
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
			results.push((id, error));
		}
		Ok(results)
	}
}
